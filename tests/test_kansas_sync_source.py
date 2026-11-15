"""Kansas source-side sync helper: legacy and isolated (broker) behaviour; no database or SSH."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts' / 'state-sync'
sys.path.insert(0, str(SCRIPTS))


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


source = load('kansas_sync_source')
ssh = load('kansas_sync_ssh')


class Completed:
    def __init__(self, stdout=''):
        self.stdout = stdout
        self.returncode = 0


class RestrictedEntryPoints(unittest.TestCase):
    def test_restricted_ssh_key_cannot_run_a_worker_or_shell(self):
        for command in ['export-worker', 'manual-export-worker', 'manual-start', 'bash -c id', 'archive ../../etc/passwd', 'describe ../latest']:
            with self.subTest(command=command), patch.dict(source.os.environ, {'SSH_ORIGINAL_COMMAND': command}):
                with self.assertRaises(ValueError):
                    source.main()

    def test_isolated_entry_point_hands_only_allowed_commands_to_the_fixed_helper(self):
        export_id = str(uuid.uuid4())
        for original, expected in [('export', ['export']), ('preflight', ['preflight']),
                                   ('describe ' + export_id, ['describe', export_id]),
                                   ('archive ' + export_id, ['archive', export_id]), ('ack ' + export_id, ['ack', export_id])]:
            with self.subTest(command=original), patch.dict(ssh.os.environ, {'SSH_ORIGINAL_COMMAND': original}, clear=True), \
                    patch.object(ssh.os, 'execv') as execv:
                ssh.main()
            execv.assert_called_once_with('/usr/bin/sudo', ['/usr/bin/sudo', '-n', '/usr/local/libexec/seo_ks_isolation.py', '--sync', *expected])

    def test_isolated_entry_point_refuses_workers_shells_paths_and_direct_use(self):
        for command in ['export-worker', 'manual-export-worker', 'manual-start', 'manual-status', 'bash -c id', 'export now',
                        'archive ../../etc/passwd', 'describe ../latest', 'ack', 'ack ' + str(uuid.uuid4()).upper()]:
            with self.subTest(command=command), patch.dict(ssh.os.environ, {'SSH_ORIGINAL_COMMAND': command}, clear=True), \
                    patch.object(ssh.os, 'execv') as execv:
                with self.assertRaises(ValueError):
                    ssh.main()
            execv.assert_not_called()
        with patch.dict(ssh.os.environ, {}, clear=True), patch.object(ssh.os, 'execv') as execv:
            with self.assertRaises(ValueError):
                ssh.main()
        execv.assert_not_called()


class BrokerMode(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.requests = Path(self.temp.name) / 'job-requests'
        self.requests.mkdir()
        self.patches = [patch.object(source, 'RUNNER', 'broker'), patch.object(source, 'REQUESTS', self.requests)]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.temp.cleanup()

    def states(self, values):
        """Simulated fixed export unit: each poll returns the next state."""
        values = list(values)

        def show(unit):
            self.assertEqual(unit, source.UNIT)
            if len(values) > 1:
                value = values.pop(0)
            else:
                value = values[0]
            if callable(value):
                value = value()
            return value
        return show

    def consume(self, state):
        def step():
            (self.requests / 'export').unlink(missing_ok=True)
            return state
        return step

    def test_export_requests_the_fixed_unit_and_waits_for_its_new_run(self):
        idle = {'ActiveState': 'inactive', 'Result': 'success', 'ExecMainStartTimestampMonotonic': '100'}
        running = {'ActiveState': 'active', 'Result': 'success', 'ExecMainStartTimestampMonotonic': '200'}
        done = {'ActiveState': 'inactive', 'Result': 'success', 'ExecMainStartTimestampMonotonic': '200'}
        seen = []
        with patch.object(source, 'unit_state', side_effect=self.states([idle, lambda: (seen.append((self.requests / 'export').read_text()), idle)[1], self.consume(running), done])), \
                patch.object(source.subprocess, 'run') as run, patch.object(source.time, 'sleep'):
            source.start_export()
        self.assertEqual(seen, ['export\n'])
        run.assert_not_called()  # never the deployment user's service manager
        self.assertFalse((self.requests / 'export').exists())

    def test_export_failure_is_reported(self):
        idle = {'ActiveState': 'inactive', 'Result': 'success', 'ExecMainStartTimestampMonotonic': '100'}
        failed = {'ActiveState': 'failed', 'Result': 'exit-code', 'ExecMainStartTimestampMonotonic': '300'}
        with patch.object(source, 'unit_state', side_effect=self.states([idle, self.consume(failed)])), patch.object(source.time, 'sleep'):
            with self.assertRaisesRegex(RuntimeError, 'export unit failed'):
                source.start_export()

    def test_running_export_is_joined_not_repeated(self):
        running = {'ActiveState': 'active', 'Result': 'success', 'ExecMainStartTimestampMonotonic': '100'}
        done = {'ActiveState': 'inactive', 'Result': 'success', 'ExecMainStartTimestampMonotonic': '100'}
        with patch.object(source, 'unit_state', side_effect=self.states([running, running, done])), patch.object(source.time, 'sleep'):
            source.start_export()
        self.assertFalse((self.requests / 'export').exists())

    def test_old_completed_run_does_not_count_as_the_requested_export(self):
        idle = {'ActiveState': 'inactive', 'Result': 'success', 'ExecMainStartTimestampMonotonic': '100'}
        with patch.object(source, 'unit_state', return_value=idle), patch.object(source.time, 'sleep'):
            with self.assertRaises(TimeoutError):
                source.start_export(timeout=0.05, interval=0.01)

    def test_manual_start_and_status_use_the_request_and_system_unit(self):
        reports = Path(self.temp.name) / 'reports'
        with patch.object(source, 'MANUAL_LATEST', reports / 'manual-latest.json'), \
                patch.object(source, 'unit_state', return_value={'ActiveState': 'inactive'}) as state, \
                patch.object(source.subprocess, 'run') as run:
            self.assertFalse(source.manual_status()['active'])
            started = source.manual_start()
        self.assertTrue(started['active'])
        self.assertEqual((self.requests / 'manual-export').read_text(), 'manual-export\n')
        run.assert_not_called()
        state.assert_called_with(source.MANUAL_UNIT)

    def test_manual_upload_uses_the_private_upload_configuration_and_export_home(self):
        root = Path(self.temp.name)
        export_id = str(uuid.uuid4())
        exports, reports = root / 'exports', root / 'reports'
        (exports / export_id).mkdir(parents=True)
        reports.mkdir()
        latest = {'export_id': export_id, 'snapshot_filter': 'non_test_ingestion_v2', 'records_total': 3, 'records_by_entity': {'wells': 1}}
        (reports / 'latest.json').write_text(json.dumps(latest))
        locks, calls = [], []
        def lock(state, *, home=None, shared=False):
            locks.append(home)
            return __import__('contextlib').nullcontext()
        with patch.multiple(source, ROOT=exports, REPORTS=reports, LATEST=reports / 'latest.json', MANUAL_LATEST=reports / 'manual-latest.json',
                            SSH_CONFIG='/etc/future-wells-ks/upload.conf', EXPORT_HOME='/var/lib/future-wells-ks/backend-data/export-home'), \
                patch.object(source, 'start_export') as exported, patch.object(source, 'export_lock', side_effect=lock), \
                patch.object(source, 'retain_latest', return_value={'kept': 1}) as retain, \
                patch.object(source.subprocess, 'run', side_effect=lambda args, **kw: calls.append(args) or Completed()):
            source.manual_export_worker()
        exported.assert_called_once_with()
        self.assertEqual(len(calls), 2)
        for args in calls:
            self.assertEqual(args[:3], ['rsync', '-a', '-e'])
            self.assertIn('-F /etc/future-wells-ks/upload.conf', args[3])
            self.assertNotIn('/home/yaro/.ssh', args[3])
        self.assertTrue(calls[1][-1].endswith(export_id + '/manifest.json'))  # manifest published last
        self.assertEqual(set(locks), {'/var/lib/future-wells-ks/backend-data/export-home'})
        self.assertEqual(retain.call_args.kwargs['home'], '/var/lib/future-wells-ks/backend-data/export-home')
        self.assertEqual(json.loads((reports / 'manual-latest.json').read_text())['status'], 'completed')


class LegacyMode(unittest.TestCase):
    def test_legacy_defaults_are_unchanged(self):
        self.assertEqual(source.RUNNER, 'user')
        self.assertIsNone(source.EXPORT_HOME)
        self.assertEqual(source.SSH_CONFIG, '/home/yaro/.ssh/config')
        with patch.object(source.subprocess, 'run') as run:
            source.start_export()
        run.assert_called_once_with(['systemctl', '--user', 'start', source.UNIT], check=True)


if __name__ == '__main__':
    unittest.main()
