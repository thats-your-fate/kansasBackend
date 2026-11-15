#!/usr/bin/env python3
"""Keep one verified source export after successful creation/delivery.

Source packages only: never touches database rows, raw downloads, or the
platform inbox (which has its own import/acknowledgement retention policy).
"""
import argparse
from contextlib import contextmanager
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import uuid

ENTITIES = {'counties', 'operators', 'regulatory_areas', 'wells', 'well_identifiers',
            'wellbores', 'wellbore_geometries', 'well_geometries', 'regulatory_records',
            'documents', 'document_contents', 'events', 'event_document_links',
            'production_monthly', 'tombstones'}

STATES = {
    'KS': ('kansas', 'kansasBackend', 'kansas_backend'),
    'NM': ('new-mexico', 'newMexicoBackend', 'new_mexico_backend'),
    'OK': ('oklahoma', 'oklahomaBackend', 'oklahoma_backend'),
    'UT': ('utah', 'utahBackend', 'utah_backend'),
    'WY': ('wyoming', 'wyomingBackend', 'wyoming_backend'),
    'CO': ('colorado', 'coloradoBackend', 'colorado_backend'),
    'ND': ('north-dakota', 'northDakotaBackend', 'north_dakota_backend'),
    'TX': ('texas', 'futurewellstexasbackend', 'texas_backend'),
}


@contextmanager
def export_lock(state, *, home=None, shared=False):
    """Serialize shell/admin exporters and protect Kansas archive readers."""
    slug = STATES[state][0]
    root = Path(home or Path.home()) / 'state-sync/export-locks'
    root.mkdir(parents=True, exist_ok=True)
    with (root / (slug + '.lock')).open('a') as handle:
        fcntl.flock(handle, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        yield


def roots_for(state, extra=(), *, home=None):
    home = Path(home or Path.home())
    slug, project, _ = STATES[state]
    values = [home / 'state-sync/exports' / slug,
              home / 'projects' / project / 'data/state-exports', *map(Path, extra)]
    roots = []
    for value in values:
        value = value.absolute()
        # Refuse symlinked roots/ancestors, not just a symlinked package.
        if any(p.is_symlink() for p in (value, *value.parents)):
            raise ValueError('Symlinked export root refused')
        if value.exists() and value not in roots:
            if not value.is_dir() or len(value.parts) < 4:
                raise ValueError('Invalid export root')
            roots.append(value)
    return roots


def manifest(package, state):
    if package.is_symlink() or not package.is_dir():
        raise ValueError('Real export directory required')
    if str(uuid.UUID(package.name)) != package.name:
        raise ValueError('Canonical export UUID required')
    path = package / 'manifest.json'
    if path.is_symlink() or not path.is_file():
        raise ValueError('Completed manifest required')
    raw = path.read_bytes()
    value = json.loads(raw)
    if (value.get('export_id') != package.name
            or value.get('schema_version') != 'state-export-v1'
            or value.get('state_code') != state
            or value.get('source_code') != STATES[state][2]
            or value.get('export_mode') not in ('full', 'baseline', 'incremental')
            or not isinstance(value.get('files'), list) or not value['files']):
        raise ValueError('Export identity/schema mismatch')
    created = datetime.fromisoformat(value['created_at'].replace('Z', '+00:00'))
    if created.tzinfo is None:
        raise ValueError('Export timestamp must include timezone')
    return value, raw, created


def verify(package, state):
    value, raw, created = manifest(package, state)
    if value['export_mode'] in ('full', 'baseline') and not ENTITIES.issubset(
            {part.get('entity_type') for part in value['files']}):
        raise ValueError('Incomplete full export; previous package retained')
    checksum = package / 'manifest.sha256'
    if checksum.is_symlink() or not checksum.is_file():
        raise ValueError('Manifest checksum required')
    if checksum.read_text().split()[0] != hashlib.sha256(raw).hexdigest():
        raise ValueError('Manifest checksum mismatch')
    seen = set()
    for part in value['files']:
        name = PurePosixPath(part['filename'])
        if name.is_absolute() or '..' in name.parts or str(name) in seen:
            raise ValueError('Unsafe or duplicate export part')
        seen.add(str(name))
        path = package / str(name)
        if any(p.is_symlink() for p in (path, *path.parents)) or not path.is_file():
            raise ValueError('Missing or symlinked export part')
        expected = part['sha256_checksum']
        if not re.fullmatch('[0-9a-f]{64}', expected):
            raise ValueError('Invalid part checksum')
        digest = hashlib.sha256()
        with path.open('rb') as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b''):
                digest.update(block)
        if digest.hexdigest() != expected:
            raise ValueError('Export part checksum mismatch')
    return created


def retain_latest(state, extra=(), *, home=None, apply=True):
    """Caller holds export_lock; only run after successful export/upload.

    An incremental package can replace local copies after upload because its
    predecessor chain is already on the platform; its inbox retention is separate.
    Partial, unrecognized and other-state directories are deliberately untouched.
    """
    packages = []
    for root in roots_for(state, extra, home=home):
        for path in root.iterdir():
            if path.name.startswith('.') or not path.is_dir() or path.is_symlink():
                continue
            try:
                _, raw, created = manifest(path, state)
            except (ValueError, KeyError, OSError, TypeError):
                continue
            packages.append((created, path.name, str(path), path, raw))
    if not packages:
        return {'state': state, 'retention': 'no_completed_export', 'removed': []}
    packages.sort(key=lambda row: row[:3])
    keep = packages[-1][3]
    # Validate every byte of the replacement before deleting anything.
    verify(keep, state)
    removed = []
    reclaimed = 0
    for _, _, _, path, raw in packages[:-1]:
        # Recheck identity immediately before deletion; never follow links.
        if path.is_symlink() or (path / 'manifest.json').read_bytes() != raw:
            raise ValueError('Export changed during retention')
        size = sum(p.stat().st_size for p in path.rglob('*') if p.is_file() and not p.is_symlink())
        if apply:
            shutil.rmtree(path)
        reclaimed += size
        removed.append(str(path))
    return {'state': state, 'retention': 'applied' if apply else 'preview',
            'kept': str(keep), 'removed': removed, 'removed_bytes': reclaimed}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['run', 'prune'])
    parser.add_argument('--state', required=True, choices=STATES)
    parser.add_argument('--root', action='append', default=[])
    parser.add_argument('--apply', action='store_true')
    # Arguments following -- belong exclusively to the export command.
    import sys
    args_list = sys.argv[1:]
    split = args_list.index('--') if '--' in args_list else len(args_list)
    args = parser.parse_args(args_list[:split])
    command = args_list[split + 1:]
    with export_lock(args.state):
        if args.action == 'run':
            if not command:
                parser.error('run requires -- COMMAND')
            env = os.environ.copy()
            env['STATE_EXPORT_RETENTION_LOCKED'] = args.state
            completed = subprocess.run(command, env=env)
            if completed.returncode:
                raise SystemExit(completed.returncode)
        result = retain_latest(args.state, args.root, apply=args.action == 'run' or args.apply)
        # stderr avoids interfering with exporter JSON consumers.
        print(json.dumps(result), file=sys.stderr, flush=True)


if __name__ == '__main__':
    main()
