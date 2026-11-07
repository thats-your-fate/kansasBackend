import unittest
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


if __name__ == '__main__':
    unittest.main()
