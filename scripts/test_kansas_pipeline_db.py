"""Run only against an explicitly configured disposable native PostgreSQL DB.

Schema migrations must already be applied. No live source requests or R2 writes.
"""
import json
import os
import unittest
import uuid

from kansas_common import query, rows, sql
from kansas_documents import publish


@unittest.skipUnless(os.environ.get('KS_TEST_DATABASE_URL'), 'set a disposable KS_TEST_DATABASE_URL')
class DocumentPublication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['DATABASE_URL'] = os.environ['KS_TEST_DATABASE_URL']

    def setUp(self):
        kid = str(uuid.uuid4().int)[:10]
        api = '15' + str(uuid.uuid4().int)[:12]
        run = query(f"""INSERT INTO ks_ingestion_run(source_id,source_url,retrieved_at,payload_checksum,
          parser_version,status,row_count,completed_at)
          SELECT id,'test-fixture',now(),{sql(str(uuid.uuid4()))},'test-fixture','completed',500001,now()
          FROM ks_source WHERE code='KGS_WELLS_ZIP' RETURNING id;""")
        well = query(f"""INSERT INTO ks_well(api14,api_raw,kgs_kid,api_validation_status,well_identity_match_method,
          lifecycle_status,source_format,source_ingestion_run_id)
          VALUES({sql(api)},{sql(api)},{sql(kid)},'valid','api14','planned','zip',{sql(run)}::uuid)
          RETURNING internal_well_id;""")
        self.well = dict(id=well, api14=api, kgs_kid=kid)
        self.item = dict(url='https://kgsimages.blob.core.windows.net/kolar/public/'+str(uuid.uuid4())+'.pdf',
                         label='Intent To Drill Well',received='2026-08-06')

    def test_publication_retry_and_revision_keep_one_document_and_deduplicated_evidence(self):
        a = publish(self.well,self.item,b'%PDF-first',2,'kansas/documents/sha256/test.pdf',None)
        b = publish(self.well,self.item,b'%PDF-first',2,'kansas/documents/sha256/test.pdf',None)
        self.assertEqual(a,b)
        stats = rows(f"""SELECT count(*) AS versions FROM ks_document_version WHERE document_id={sql(a['document_id'])}::uuid""")[0]
        self.assertEqual(stats['versions'],1)
        self.assertEqual(a['events'],2)
        publish(self.well,self.item,b'%PDF-revised',3,'kansas/documents/sha256/revised.pdf',None)
        stats = rows(f"""SELECT count(*) AS versions FROM ks_document_version WHERE document_id={sql(a['document_id'])}::uuid""")[0]
        self.assertEqual(stats['versions'],2)
        evidence=rows(f"""SELECT count(*) AS links FROM ks_document_event_link WHERE document_id={sql(a['document_id'])}::uuid""")[0]
        self.assertEqual(evidence['links'],2)

    def test_mismatched_or_tombstoned_well_rolls_back_without_document(self):
        bad = dict(self.well,api14='15163245390000')
        with self.assertRaises(RuntimeError):
            publish(bad,self.item,b'%PDF',1,'kansas/documents/sha256/test.pdf',None)
        query('UPDATE ks_well SET is_tombstoned=true WHERE internal_well_id='+sql(self.well['id'])+'::uuid;')
        with self.assertRaises(RuntimeError):
            publish(self.well,self.item,b'%PDF',1,'kansas/documents/sha256/test.pdf',None)

    def test_plugging_application_cannot_emit_a_plugging_event(self):
        item = dict(self.item,label='Plugging Application')
        doc=publish(self.well,item,b'%PDF',1,'kansas/documents/sha256/test.pdf',None)
        events=rows(f"""SELECT e.event_type FROM ks_document_event_link l JOIN ks_event e ON e.id=l.event_id
          WHERE l.document_id={sql(doc['document_id'])}::uuid""")
        self.assertEqual([e['event_type'] for e in events],['DOCUMENT_ADDED'])

    def test_completion_receipt_is_separate_from_unknown_effective_date(self):
        doc=publish(self.well,dict(self.item,label='Well Completion Report'),b'%PDF',1,'kansas/documents/sha256/test.pdf',None)
        e=rows(f"""SELECT e.occurred_at,e.published_at,e.payload FROM ks_event e JOIN ks_document_event_link l ON l.event_id=e.id
          WHERE l.document_id={sql(doc['document_id'])}::uuid AND e.event_type='COMPLETION_RECORDED'""")[0]
        self.assertIsNone(e['occurred_at'])
        self.assertTrue(e['published_at'].startswith('2026-08-06'))
        self.assertIsNone(e['payload']['effective_date'])


if __name__ == '__main__':
    unittest.main()
