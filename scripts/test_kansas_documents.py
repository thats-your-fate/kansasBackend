import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
import kansas_documents
from kansas_documents import allowed_pdf, api14, classify, parse_page, effective_date

URL = 'https://kgsimages.blob.core.windows.net/kolar/filer_3/public/2026-08/kolar_images.images.img/sid=1322168976.pdf'
PAGE = f'''<h1>API: 15-163-24539</h1><div>KID: 1057551044</div>
<a href="https://www.kgslibrary.com/index.cfm?whk=1057551044">WDL login required</a>
<ul><li><a href="{URL}">Intent To Drill Well</a> (received by KGS Aug 06, 2026)</li></ul>'''


class PublicDocuments(unittest.TestCase):
    def test_live_page_shape_with_receipt_and_restricted_library(self):
        self.assertEqual(parse_page(PAGE, '1057551044', '15163245390000'),
                         [dict(url=URL, label='Intent To Drill Well', received='2026-08-06')])

    def test_identity_conflicts_and_missing_identity_fail_closed(self):
        for body, kid, api in [(PAGE,'1','15163245390000'),(PAGE,'1057551044','15163245400000'),
                               ('maintenance page','1057551044','15163245390000')]:
            with self.assertRaises(ValueError):
                parse_page(body,kid,api)

    def test_missing_and_future_receipt_are_not_guessed_from_url(self):
        for body in [PAGE.replace('(received by KGS Aug 06, 2026)',''),PAGE.replace('Aug 06, 2026','Aug 06, 2099')]:
            with self.assertRaises(ValueError):
                parse_page(body,'1057551044','15163245390000')

    def test_pdf_allowlist_rejects_private_and_lookalike_hosts(self):
        self.assertTrue(allowed_pdf(URL))
        for url in [URL.replace('/public/','/private/'), URL.replace('https:','http:'),
                    URL.replace('windows.net','windows.net.evil.com'), URL+'?token=private',
                    'https://www.kgslibrary.com/test.pdf',URL.replace('https://','https://user:pass@')]:
            self.assertFalse(allowed_pdf(url))

    def test_plugging_application_does_not_assert_plugged_well(self):
        self.assertEqual(classify('Plugging Application'),('cp1',None))
        self.assertEqual(classify('Well Plugging Report'),('cp4','PLUGGING_RECORDED'))
        self.assertEqual(classify('Well Completion Report'),('aco1','COMPLETION_RECORDED'))
        self.assertEqual(classify('Intent To Drill Well'),('c1','INTENT_POSTED'))

    def test_effective_date_requires_explicit_unambiguous_field(self):
        self.assertEqual(effective_date('Completion Date: 07/01/2026','aco1'),'2026-07-01')
        for text in ['RECEIVED 07/01/2026','Completion Date: 07/01/2026 Completion Date: 07/02/2026',
                     'Completion Date: 99/99/2026']:
            self.assertIsNone(effective_date(text,'aco1'))

    def test_duplicate_pdf_links_deduplicate_and_conflicting_dates_fail(self):
        self.assertEqual(len(parse_page(PAGE+PAGE,'1057551044','15163245390000')),1)
        with self.assertRaises(ValueError):
            parse_page(PAGE+PAGE.replace('Aug 06','Aug 07'),'1057551044','15163245390000')

    def test_explicit_restriction_overrides_public_url(self):
        body=PAGE.replace('Intent To Drill Well</a>', 'Confidential Intent To Drill Well</a>')
        self.assertEqual(parse_page(body,'1057551044','15163245390000'),[])

    def test_api_suffix_is_preserved(self):
        self.assertEqual(api14('15-163-24539'),'15163245390000')
        self.assertEqual(api14('15-163-24539-0100'),'15163245390100')
        with self.assertRaises(ValueError): api14('43-047-15065')

    def test_conflicting_pdf_is_quarantined_and_next_document_continues(self):
        well = dict(id='well-id', kgs_kid='1057551044', api14='15163245390000')
        second = URL.replace('1322168976', '1322168977')
        items = [dict(url=u, label='Intent To Drill Well', received='2026-08-06') for u in (URL, second)]
        text_calls = []
        def run(command, **kwargs):
            from types import SimpleNamespace
            if command[0] == 'pdfinfo': return SimpleNamespace(stdout='Pages: 1')
            Path(command[-1]).write_text('API 15-001-00001' if not text_calls else 'API 15-163-24539')
            text_calls.append(1)
            return SimpleNamespace()
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(kansas_documents, 'ROOT', Path(directory)), \
             patch.object(kansas_documents, 'parse_page', return_value=items), \
             patch.object(kansas_documents, 'fetch', side_effect=[b'page', b'%PDF-test%%EOF', b'%PDF-test%%EOF']), \
             patch.object(kansas_documents, 'rows', return_value=[]), \
             patch.object(kansas_documents, 'publish', return_value={'document_id': 'valid'}), \
             patch.object(kansas_documents.subprocess, 'run', side_effect=run), \
             patch('builtins.print'):
            from unittest.mock import Mock
            storage = Mock()
            result = kansas_documents.process(well, storage, kansas_documents.date.fromisoformat('2025-01-01'))
            self.assertEqual(result[2], [{'document_id': 'valid'}])
            self.assertEqual(len(list((Path(directory)/'data/kansas/documents/review').glob('*.json'))), 1)
            storage.store.assert_called_once()

    def test_oversized_pdf_stays_bounded_and_requires_review(self):
        from unittest.mock import MagicMock
        opener = MagicMock()
        opener.open.return_value.__enter__.return_value.read.return_value = b'x'*11
        with patch.object(kansas_documents, 'build_opener', return_value=opener):
            with self.assertRaises(kansas_documents.DocumentReview):
                kansas_documents.fetch(URL, 10)
            opener.open.return_value.__enter__.return_value.read.assert_called_once_with(11)

    def test_timeout_retries_are_bounded_and_can_recover(self):
        from unittest.mock import MagicMock
        opener, response = MagicMock(), MagicMock()
        response.__enter__.return_value.read.return_value = b'%PDF-recovered'
        opener.open.side_effect = [TimeoutError('source timeout'), TimeoutError('source timeout'), response]
        with patch.object(kansas_documents, 'build_opener', return_value=opener), patch.object(kansas_documents.time, 'sleep'):
            self.assertEqual(kansas_documents.fetch(URL, 100), b'%PDF-recovered')
        self.assertEqual(opener.open.call_count, 3)

    def test_time_budget_checkpoints_unprocessed_wells_without_marking_them_failed(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(kansas_documents, 'ROOT', Path(directory)), \
             patch.object(kansas_documents, 'load_env'), \
             patch.object(kansas_documents, 'R2'), \
             patch.object(kansas_documents, 'rows', return_value=[{'id': 'a'}, {'id': 'b'}]), \
             patch.object(kansas_documents, 'process') as process, \
             patch.object(kansas_documents.time, 'monotonic', side_effect=[0, 21601]), \
             patch('sys.argv', ['kansas_documents.py']), patch('builtins.print'):
            kansas_documents.main()
            report = json.loads((Path(directory)/'data/kansas/documents/latest-run.json').read_text())
            self.assertTrue(report['budget_exhausted'])
            self.assertEqual(report['remaining'], 2)
            self.assertEqual(report['failed'], 0)
            process.assert_not_called()


if __name__ == '__main__':
    unittest.main()
