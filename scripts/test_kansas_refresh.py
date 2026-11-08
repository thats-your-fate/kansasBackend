import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import kansas_refresh


class InventoryRefresh(unittest.TestCase):
    def test_unchanged_successful_snapshot_does_not_advance_tombstones_again(self):
        run = 'e21dec71-3084-44e6-bec4-1ab2d93cfc47'
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(kansas_refresh, 'ROOT', Path(directory)), \
             patch.object(kansas_refresh, 'rows', return_value=[dict(status='completed', row_count=477000)]), \
             patch.object(kansas_refresh, 'cli', return_value='run_id: '+run) as cli:
            path = Path(directory)/'data/kansas/wells/last-success.json'
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(dict(result=dict(run_id=run))))
            self.assertEqual(kansas_refresh.refresh_wells(), dict(run_id=run, unchanged=True))
            self.assertEqual(cli.call_count, 1)
            self.assertEqual(cli.call_args.args[:2], ('snapshot-wells', '--zip'))
