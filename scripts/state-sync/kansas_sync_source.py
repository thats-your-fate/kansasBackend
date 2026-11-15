#!/usr/bin/env python3
"""Restricted SSH interface to the Kansas exporter; runs on the state server."""
import fcntl
import json
from export_package_retention import export_lock, retain_latest
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone

BACKEND = Path('/home/yaro/projects/kansasBackend')
BINARY = BACKEND / 'target/release/ks_ingest'
ROOT = Path('/home/yaro/state-sync/exports/kansas')
REPORTS = Path('/home/yaro/state-sync/export-runs/kansas')
LATEST = REPORTS / 'latest.json'
UNIT = 'future-wells-ks-export.service'
MANUAL_UNIT = 'future-wells-ks-manual-export.service'
MANUAL_LATEST = REPORTS / 'manual-latest.json'
# Isolated runtime (fw-ks-runtime): no user manager. A fixed root-owned path
# unit starts the export/manual-export units when a request file appears.
RUNNER = os.environ.get('KS_SYNC_RUNNER', 'user')
REQUESTS = Path(os.environ.get('KS_JOB_REQUEST_DIR', '/nonexistent/job-requests'))
# Lock/retention home; must not contain symlinked ancestors (retention refuses them).
EXPORT_HOME = os.environ.get('KS_EXPORT_HOME') or None
SSH_CONFIG = os.environ.get('KS_SYNC_SSH_CONFIG', '/home/yaro/.ssh/config')


def now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def parse_json(text):
    for line in reversed(text.splitlines()):
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and value.get('success'):
            return value
    raise RuntimeError('exporter did not return successful JSON')


def cli(*args):
    result = subprocess.run([str(BINARY), 'state-export-v1', *args], cwd=BACKEND,
                            stdout=subprocess.PIPE, text=True, check=True)
    return parse_json(result.stdout)


def unit_state(unit):
    result = subprocess.run(['systemctl', 'show', unit, '-p', 'ActiveState', '-p', 'Result',
                             '-p', 'ExecMainStartTimestampMonotonic'], capture_output=True, text=True, check=True, timeout=30)
    return dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)


def request(kind):
    temporary = REQUESTS / ('.' + kind + '-' + uuid.uuid4().hex)
    temporary.write_text(kind + '\n')
    temporary.replace(REQUESTS / kind)


def start_export(timeout=5 * 3600, interval=5):
    """Run the export unit to completion, like systemctl start: join a running export."""
    if RUNNER != 'broker':
        subprocess.run(['systemctl', '--user', 'start', UNIT], check=True)
        return
    state = unit_state(UNIT)
    joined = state.get('ActiveState') in ('active', 'activating')
    before = int(state.get('ExecMainStartTimestampMonotonic') or 0)
    if not joined:
        request('export')
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(interval)
        state = unit_state(UNIT)
        started = joined or int(state.get('ExecMainStartTimestampMonotonic') or 0) > before
        if started and not (REQUESTS / 'export').exists() and state.get('ActiveState') in ('inactive', 'failed'):
            if state.get('Result') != 'success':
                raise RuntimeError('Kansas export unit failed')
            return
    raise TimeoutError('Kansas export did not finish')


def export_worker():
    ROOT.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)
    with export_lock('KS', home=EXPORT_HOME):
        with (REPORTS / 'export.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = cli('full', '--consumer', 'future-wells-co', '--output-dir', str(ROOT),
                         '--max-records-per-part', '5000', '--exclude-test-ingestion', '--json')
            cli('verify', result.get('package_path') or result['output_path'], '--json')
            result['snapshot_filter'] = 'non_test_ingestion_v2'
            atomic_json(REPORTS / (result['export_id'] + '.json'), result)
            atomic_json(LATEST, result)
            print(json.dumps(result), flush=True)


def export():
    # Reuse a delivered-but-unacknowledged package after a failed transfer/import.
    latest = json.loads(LATEST.read_text()) if LATEST.exists() else None
    if not latest or latest.get('acknowledged') or latest.get('snapshot_filter') != 'non_test_ingestion_v2':
        start_export()
        latest = json.loads(LATEST.read_text())
    print(json.dumps(latest))


def identifier(value):
    if str(uuid.UUID(value)) != value:
        raise ValueError('canonical UUID required')
    return value


def describe(export_id):
    return json.loads((REPORTS / (identifier(export_id) + '.json')).read_text())


def manual_status():
    if RUNNER == 'broker':
        active = 'activating' if (REQUESTS / 'manual-export').exists() else unit_state(MANUAL_UNIT).get('ActiveState', '')
    else:
        active = subprocess.run(['systemctl', '--user', 'is-active', MANUAL_UNIT],
                                capture_output=True, text=True).stdout.strip()
    job = json.loads(MANUAL_LATEST.read_text()) if MANUAL_LATEST.exists() else None
    if job and job.get('status') == 'running' and active not in ('active', 'activating'):
        job.update(status='failed', error='Export job stopped before completion; start a new export.')
    return {'success': True, 'active': active in ('active', 'activating'), 'job': job}


def manual_start():
    if manual_status()['active']:
        return manual_status()
    if RUNNER == 'broker':
        request('manual-export')
    else:
        subprocess.run(['systemctl', '--user', 'start', '--no-block', MANUAL_UNIT], check=True)
    return manual_status()


def manual_export_worker():
    REPORTS.mkdir(parents=True, exist_ok=True)
    job = {'status': 'running', 'phase': 'exporting', 'started_at': now()}

    def save(phase):
        job.update(phase=phase, updated_at=now())
        atomic_json(MANUAL_LATEST, job)

    try:
        save('exporting')
        start_export()
        with export_lock('KS', home=EXPORT_HOME, shared=True):
            exported = json.loads(LATEST.read_text())
            export_id = identifier(exported['export_id'])
            if exported.get('snapshot_filter') != 'non_test_ingestion_v2':
                raise ValueError('a production Kansas snapshot is required')
            job.update(export_id=export_id, records_total=exported['records_total'],
                       wells=exported['records_by_entity']['wells'])
            save('uploading')
            package = ROOT / export_id
            destination = 'futurewellsco-state-sync:kansas/' + export_id + '/'
            rsync = ['rsync', '-a', '-e', 'ssh -F ' + shlex.quote(SSH_CONFIG) + ' -o BatchMode=yes -o StrictHostKeyChecking=yes']
            # Publish the manifest last, so the import UI cannot select a partial upload.
            subprocess.run([*rsync, '--exclude=manifest.json', str(package) + '/', destination], check=True, timeout=3600)
            subprocess.run([*rsync, str(package / 'manifest.json'), destination + 'manifest.json'], check=True, timeout=300)
        with export_lock('KS', home=EXPORT_HOME):
            print(json.dumps(retain_latest('KS', [ROOT], home=EXPORT_HOME)), file=sys.stderr, flush=True)
        job.update(status='completed', completed_at=now(), uploaded=True)
        save('completed')
    except Exception as error:
        job.update(status='failed', error=str(error))
        save('failed')
        raise


def acknowledge(export_id):
    with export_lock('KS', home=EXPORT_HOME):
        report_path = REPORTS / (export_id + '.json')
        report = json.loads(report_path.read_text())
        if not report.get('acknowledged'):
            try:
                cli('acknowledge', '--consumer', 'future-wells-co', '--export-id', export_id, '--json')
            except subprocess.CalledProcessError:
                # Recover if the DB acknowledgement succeeded before this report was saved.
                status = cli('status', '--consumer', 'future-wells-co', '--json')['status_text']
                if not any(line.strip().startswith(export_id + ' ') and 'acknowledged_at=Some(' in line
                           for line in status.splitlines()):
                    raise
            report['acknowledged'] = True
            atomic_json(report_path, report)
            if LATEST.exists() and json.loads(LATEST.read_text())['export_id'] == export_id:
                atomic_json(LATEST, report)
        print(json.dumps(retain_latest('KS', [ROOT], home=EXPORT_HOME)), file=sys.stderr, flush=True)
        print(json.dumps({'success': True, 'export_id': export_id, 'acknowledged': True}))


def main():
    args = shlex.split(os.environ['SSH_ORIGINAL_COMMAND']) if 'SSH_ORIGINAL_COMMAND' in os.environ else sys.argv[1:]
    if args == ['export-worker'] and 'SSH_ORIGINAL_COMMAND' not in os.environ:
        export_worker()
    elif args == ['manual-export-worker'] and 'SSH_ORIGINAL_COMMAND' not in os.environ:
        manual_export_worker()
    elif args == ['manual-start'] and 'SSH_ORIGINAL_COMMAND' not in os.environ:
        print(json.dumps(manual_start()))
    elif args == ['manual-status'] and 'SSH_ORIGINAL_COMMAND' not in os.environ:
        print(json.dumps(manual_status()))
    elif args == ['export']:
        export()
    elif args == ['preflight']:
        subprocess.run([str(BINARY), 'state-export-v1', 'preflight'], cwd=BACKEND, check=True)
    elif len(args) == 2 and args[0] == 'archive':
        export_id = identifier(args[1])
        with export_lock('KS', home=EXPORT_HOME, shared=True):
            package = ROOT / export_id
            if package.is_symlink() or not (package / 'manifest.json').is_file():
                raise ValueError('completed Kansas package required')
            subprocess.run(['/usr/bin/tar', '-C', str(ROOT), '-cf', '-', export_id], check=True)
    elif len(args) == 2 and args[0] == 'ack':
        acknowledge(identifier(args[1]))
    elif len(args) == 2 and args[0] == 'describe':
        print(json.dumps(describe(args[1])))
    else:
        raise ValueError('allowed commands: preflight, export, describe UUID, archive UUID, ack UUID')


if __name__ == '__main__':
    main()
