import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts' / 'state-sync'))
import export_package_retention as retention


def package(root, day, state='CO', mode='full'):
    path = root / str(uuid.uuid4())
    path.mkdir(parents=True)
    (path / 'wells').mkdir()
    content = b'compressed export fixture'
    (path / 'wells/part.zst').write_bytes(content)
    value = {'export_id': path.name, 'schema_version': 'state-export-v1',
             'state_code': state, 'source_code': retention.STATES[state][2],
             'export_mode': mode, 'created_at': f'2026-11-{day:02d}T01:00:00Z',
             'files': []}
    for entity in sorted(retention.ENTITIES):
        filename = entity + '.zst'
        (path / filename).write_bytes(content)
        value['files'].append({'filename': filename, 'entity_type': entity,
                              'sha256_checksum': hashlib.sha256(content).hexdigest()})
    raw = json.dumps(value).encode()
    (path / 'manifest.json').write_bytes(raw)
    (path / 'manifest.sha256').write_text(hashlib.sha256(raw).hexdigest() + '  manifest.json\n')
    return path


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.root = self.home / 'state-sync/exports/colorado'

    def test_one_latest_across_legacy_and_current_roots(self):
        old = package(self.home / 'projects/coloradoBackend/data/state-exports', 1)
        other = package(self.root, 2)
        newest = package(self.root, 3)
        preview = retention.retain_latest('CO', home=self.home, apply=False)
        self.assertTrue(old.exists())
        self.assertEqual(len(preview['removed']), 2)
        result = retention.retain_latest('CO', home=self.home)
        self.assertEqual(result['kept'], str(newest))
        self.assertFalse(old.exists())
        self.assertFalse(other.exists())
        self.assertTrue(newest.exists())
        self.assertEqual(retention.retain_latest('CO', home=self.home)['removed'], [])

    def test_corrupt_replacement_never_deletes_previous(self):
        old = package(self.root, 1)
        new = package(self.root, 2)
        (new / 'counties.zst').write_bytes(b'truncated')
        with self.assertRaisesRegex(ValueError, 'part checksum'):
            retention.retain_latest('CO', home=self.home)
        self.assertTrue(old.exists())

    def test_corrupt_manifest_checksum_never_deletes_previous(self):
        old = package(self.root, 1)
        new = package(self.root, 2)
        (new / 'manifest.sha256').write_text('0' * 64)
        with self.assertRaisesRegex(ValueError, 'Manifest checksum'):
            retention.retain_latest('CO', home=self.home)
        self.assertTrue(old.exists())

    def test_partial_other_state_and_symlinks_are_untouched(self):
        old = package(self.root, 1)
        newest = package(self.root, 2)
        other = package(self.root, 1, state='UT')
        partial = self.root / '.partials/example'
        partial.mkdir(parents=True)
        (partial / 'data').write_text('in progress')
        outside = package(self.home / 'outside', 1)
        link = self.root / outside.name
        link.symlink_to(outside, target_is_directory=True)
        malformed = self.root / str(uuid.uuid4())
        malformed.mkdir()
        (malformed / 'manifest.json').write_text('{')
        retention.retain_latest('CO', home=self.home)
        self.assertFalse(old.exists())
        for path in [newest, other, partial, outside, link, malformed]:
            self.assertTrue(path.exists())

    def test_new_part_path_traversal_or_symlink_aborts_cleanup(self):
        for filename in ['../outside', '/etc/passwd', 'link']:
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as directory:
                home = Path(directory)
                root = home / 'state-sync/exports/colorado'
                old = package(root, 1)
                new = package(root, 2)
                (new / 'link').symlink_to(old / 'wells/part.zst')
                value = json.loads((new / 'manifest.json').read_text())
                value['files'][0]['filename'] = filename
                raw = json.dumps(value).encode()
                (new / 'manifest.json').write_bytes(raw)
                (new / 'manifest.sha256').write_text(hashlib.sha256(raw).hexdigest())
                with self.assertRaises(ValueError):
                    retention.retain_latest('CO', home=home)
                self.assertTrue(old.exists())

    def test_failed_export_or_failed_upload_does_not_prune(self):
        old = package(self.root, 1)
        package(self.root, 2)
        env = {**os.environ, 'HOME': str(self.home)}
        result = subprocess.run([sys.executable, str(Path(retention.__file__)),
            'run', '--state', 'CO', '--', 'bash', '-c', 'exit 23'], env=env, capture_output=True)
        self.assertEqual(result.returncode, 23)
        self.assertTrue(old.exists())

    def test_successful_command_prunes_and_passes_lock_marker(self):
        old = package(self.root, 1)
        new = package(self.root, 2)
        env = {**os.environ, 'HOME': str(self.home)}
        result = subprocess.run([sys.executable, str(Path(retention.__file__)),
            'run', '--state', 'CO', '--', 'bash', '-c',
            'test "$STATE_EXPORT_RETENTION_LOCKED" = CO'], env=env, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(old.exists())
        self.assertTrue(new.exists())

    def test_reader_blocks_cleanup_until_archive_finishes(self):
        import fcntl
        with retention.export_lock('CO', home=self.home, shared=True):
            code = "import fcntl,sys; f=open(sys.argv[1],'a'); fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)"
            lock = self.home / 'state-sync/export-locks/colorado.lock'
            blocked = subprocess.run([sys.executable, '-c', code, str(lock)], capture_output=True)
            self.assertNotEqual(blocked.returncode, 0)
        available = subprocess.run([sys.executable, '-c', code, str(lock)], capture_output=True)
        self.assertEqual(available.returncode, 0)

    def test_incremental_source_copy_can_be_rotated_after_delivery(self):
        old = package(self.root, 1)
        latest = package(self.root, 2, mode='incremental')
        result = retention.retain_latest('CO', home=self.home)
        self.assertEqual(result['kept'], str(latest))
        self.assertFalse(old.exists())


if __name__ == '__main__':
    unittest.main()
