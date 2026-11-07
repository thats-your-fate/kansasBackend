#!/usr/bin/env python3
"""Public KGS well pages -> exact well/API -> verified immutable R2 PDFs -> events.

Resumable bounded discovery; all writes, retries and evidence are part of this
ingestion service. A receipt date is never silently used as an effective date.
"""
import argparse
from datetime import date, datetime, timezone
import fcntl
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen, build_opener, HTTPRedirectHandler

from kansas_common import ROOT, load_env, sql, query, rows, safe_error, write_json

DETAIL = 'https://chasm.kgs.ku.edu/ords/qualified.well_page.DisplayWell?f_kid='
PDF_HOST = 'kgsimages.blob.core.windows.net'
MAX_PDF = 50 * 1024 * 1024
VERSION = 'kgs-public-pdf-v1'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def api14(value):
    digits = re.sub(r'\D', '', value or '')
    if len(digits) == 10:
        digits += '0000'
    if len(digits) != 14 or not digits.startswith('15'):
        raise ValueError('invalid Kansas API')
    return digits


def allowed_pdf(url):
    u = urlsplit(url)
    return (u.scheme == 'https' and u.hostname == PDF_HOST and not u.username
            and not u.password and not u.port and not u.query and not u.fragment
            and u.path.lower().endswith('.pdf') and '/public/' in u.path.lower())


class Page(HTMLParser):
    def __init__(self, body):
        super().__init__(convert_charrefs=True)
        self.text, self.items = [], []
        self.item = self.link = None
        self.feed(body)

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == 'li':
            self.item = {'text': '', 'links': []}
        if tag == 'a' and self.item is not None:
            self.link = {'url': a.get('href', ''), 'label': ''}

    def handle_data(self, text):
        self.text.append(text)
        if self.item is not None:
            self.item['text'] += text
        if self.link is not None:
            self.link['label'] += text

    def handle_endtag(self, tag):
        if tag == 'a' and self.link is not None:
            if self.item is not None:
                self.item['links'].append(self.link)
            self.link = None
        if tag == 'li' and self.item is not None:
            self.items.append(self.item)
            self.item = None


def parse_page(body, kid, expected_api):
    page = Page(body)
    text = ' '.join(page.text)
    found_kid = re.search(r'\bKID:\s*(\d+)', text)
    found_api = re.search(r'\bAPI:\s*(15-\d{3}-\d{5}(?:-\d{2}(?:-?\d{2})?)?)', text)
    if not found_kid or found_kid[1] != str(kid) or not found_api or api14(found_api[1]) != api14(expected_api):
        raise ValueError('KGS page identity mismatch or missing identity')
    documents = {}
    for item in page.items:
        for link in item['links']:
            if not allowed_pdf(link['url']):
                continue
            dates = re.findall(r'received by KGS\s+([A-Za-z]{3}\s+\d{1,2},\s*\d{4})', item['text'], re.I)
            received = datetime.strptime(dates[0], '%b %d, %Y').date() if len(dates) == 1 else None
            if not received or received > date.today():
                raise ValueError('public PDF has missing/ambiguous/future KGS receipt date')
            metadata = dict(url=link['url'], label=' '.join(link['label'].split()), received=str(received))
            if link['url'] in documents and documents[link['url']] != metadata:
                raise ValueError('conflicting document metadata')
            documents[link['url']] = metadata
    return list(documents.values())


def classify(label):
    label = ' '.join(label.lower().split())
    if label.startswith('intent to drill'):
        return 'c1', 'INTENT_POSTED'
    if label in ('well completion report', 'completion report', 'aco-1'):
        return 'aco1', 'COMPLETION_RECORDED'
    if label in ('well plugging report', 'well plugging record', 'plugging record', 'plugging report', 'cp-4'):
        return 'cp4', 'PLUGGING_RECORDED'
    if label in ('plugging application', 'application to plug', 'cp-1'):
        return 'cp1', None
    if label in ('well map', 'plat', 'location plat'):
        return 'plat', None
    if label in ('directional survey', 'deviation survey'):
        return 'directional_survey', None
    return 'other', None


def effective_date(text, kind):
    labels = {'c1': r'Permit\s+Date', 'aco1': r'(?:Completion\s+Date|Date\s+of\s+Completion)',
              'cp4': r'(?:Date\s+Well\s+Plugged|Plugging\s+Date)'}
    if kind not in labels:
        return None
    values = re.findall(r'\b' + labels[kind] + r'\s*:\s*(\d{1,2}/\d{1,2}/\d{4})\b', text, re.I)
    dates = set()
    for value in values:
        try:
            d = datetime.strptime(value, '%m/%d/%Y').date()
            if d <= date.today():
                dates.add(d)
        except ValueError:
            pass
    return str(next(iter(dates))) if len(dates) == 1 else None


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError('source redirect requires review')


def fetch(url, maximum):
    # Known page/PDF hosts only. Never follow a source link into a login library.
    if not (url.startswith(DETAIL) and url[len(DETAIL):].isdigit()) and not allowed_pdf(url):
        raise ValueError('unapproved source URL')
    for attempt in range(3):
        try:
            with build_opener(NoRedirect).open(Request(url, headers={'User-Agent': 'FutureWells-Kansas-PublicRecords/1.0'}), timeout=30) as r:
                content = r.read(maximum + 1)
                if len(content) > maximum:
                    raise ValueError('source response too large')
                return content
        except (OSError, TimeoutError):
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


class R2:
    def __init__(self):
        self.bucket = os.environ['R2_BUCKET_NAME']
        u = urlsplit(os.environ['R2_S3_ENPOINT'])  # Existing Kansas spelling.
        if (u.scheme != 'https' or not re.fullmatch(r'[a-f0-9]{32}(?:\.eu)?\.r2\.cloudflarestorage\.com', u.hostname or '')
                or u.username or u.password or u.query or u.fragment or u.port
                or u.path not in ('', '/', '/' + self.bucket, '/' + self.bucket + '/')):
            raise ValueError('invalid configured R2 endpoint')
        self.endpoint = 'https://' + u.hostname
        self.env = {k: v for k, v in os.environ.items() if k in ('PATH', 'LANG', 'SSL_CERT_FILE', 'SSL_CERT_DIR')}
        self.env.update(AWS_ACCESS_KEY_ID=os.environ['R2_ACCESS_KEY_ID'],
                        AWS_SECRET_ACCESS_KEY=os.environ['R2_SECRET_ACCESS_KEY'],
                        AWS_CONFIG_FILE='/dev/null', AWS_SHARED_CREDENTIALS_FILE='/dev/null',
                        AWS_EC2_METADATA_DISABLED='true', AWS_PAGER='', AWS_MAX_ATTEMPTS='2',
                        AWS_REQUEST_CHECKSUM_CALCULATION='when_required', AWS_RESPONSE_CHECKSUM_VALIDATION='when_required')

    def call(self, operation, *args, missing=False):
        command = [os.environ.get('KS_AWS_BIN', 'aws'), '--endpoint-url', self.endpoint, '--region', 'auto',
                   '--cli-connect-timeout', '10', '--cli-read-timeout', '30', 's3api', operation, '--bucket', self.bucket, *args]
        r = subprocess.run(command, env=self.env, capture_output=True, text=True, timeout=90)
        if r.returncode:
            if missing and any(x in r.stderr for x in ('(404)', '(NoSuchKey)', '(NotFound)')):
                return None
            raise RuntimeError('R2 operation failed: ' + operation)
        return json.loads(r.stdout or '{}')

    def store(self, path, checksum):
        key = f'kansas/documents/sha256/{checksum[:2]}/{checksum}.pdf'
        head = self.call('head-object', '--key', key, missing=True)
        if head is None:
            self.call('put-object', '--key', key, '--body', str(path), '--content-type', 'application/pdf',
                      '--metadata', 'sha256=' + checksum, '--if-none-match', '*')
        head = self.call('head-object', '--key', key)
        if head['ContentLength'] != path.stat().st_size or head.get('Metadata', {}).get('sha256') != checksum:
            raise RuntimeError('R2 size/checksum metadata mismatch')
        readback = path.with_suffix('.readback')
        try:
            self.call('get-object', '--key', key, str(readback))
            if digest(readback.read_bytes()) != checksum:
                raise RuntimeError('R2 read-back checksum mismatch')
        finally:
            readback.unlink(missing_ok=True)
        return key


def publish(well, item, pdf, pages, key, effective):
    """Atomic metadata, immutable version, run lineage and event evidence publication."""
    kind, event_type = classify(item['label'])
    identity = 'kgs:pdf:' + digest(item['url'].encode())
    checksum = digest(pdf)
    metadata = dict(item, api14=well['api14'], kgs_kid=well['kgs_kid'],
                    effective_date=effective, effective_date_basis='explicit_pdf_label' if effective else None,
                    match_method='exact_kgs_kid_and_api', pipeline_version=VERSION)
    metadata_json = sql(json.dumps(metadata)) + '::jsonb'
    run_hash = digest(json.dumps([identity, checksum, metadata], sort_keys=True).encode())
    source_hash = digest(json.dumps(metadata, sort_keys=True).encode())
    # The document identity is source-global. A multi-well source PDF must be
    # reviewed instead of being silently reassigned by the second well's crawl.
    statement = f"""
    BEGIN;
    SELECT pg_advisory_xact_lock(hashtext('kansas-document-publication'));
    CREATE TEMP TABLE resolved ON COMMIT DROP AS SELECT w.* FROM ks_well w
      JOIN ks_ingestion_run r ON r.id=w.source_ingestion_run_id
      WHERE w.internal_well_id={sql(well['id'])}::uuid AND w.kgs_kid={sql(well['kgs_kid'])}
       AND w.api14={sql(well['api14'])} AND NOT w.is_tombstoned AND r.status='completed'
       AND r.row_count>100000;
    DO $$ BEGIN IF (SELECT count(*) FROM resolved)<>1 THEN RAISE EXCEPTION 'well resolution changed'; END IF;
      IF EXISTS(SELECT 1 FROM ks_well_document WHERE document_source='KGS_DOCUMENTS'
        AND source_document_id={sql(identity)} AND well_id IS DISTINCT FROM {sql(well['id'])}::uuid)
      THEN RAISE EXCEPTION 'source PDF has conflicting well identity'; END IF; END $$;
    CREATE TEMP TABLE run ON COMMIT DROP AS
    WITH inserted AS (
      INSERT INTO ks_ingestion_run(source_id,source_url,retrieved_at,payload_checksum,parser_version,
        status,row_count,coverage_start,coverage_end,completed_at,manifest)
      SELECT id,{sql(item['url'])},now(),{sql(run_hash)},{sql(VERSION)},'completed',1,
        {sql(item['received'])}::date,{sql(item['received'])}::date,now(),{metadata_json}
      FROM ks_source WHERE code='KGS_DOCUMENTS'
      ON CONFLICT(source_id,payload_checksum) WHERE status IN ('running','completed') DO UPDATE SET updated_at=ks_ingestion_run.updated_at RETURNING id
    ) SELECT id FROM inserted;
    CREATE TEMP TABLE doc ON COMMIT DROP AS
    WITH inserted AS (
      INSERT INTO ks_well_document(document_source,source_document_id,document_type,document_type_raw,well_id,
        api_number_raw,kgs_kid,source_url,filing_date,filing_date_raw,public_access_state,parser_status,
        content_hash,content_object_storage_key,source_ingestion_run_id,source_record_hash,effective_date,
        effective_date_basis,stored_at,page_count)
      SELECT 'KGS_DOCUMENTS',{sql(identity)},{sql(kind)},{sql(item['label'])},internal_well_id,
        api_raw,kgs_kid,{sql(item['url'])},{sql(item['received'])}::date,{sql(item['received'])},'public','extracted',
        {sql(checksum)},{sql(key)},(SELECT id FROM run),{sql(source_hash)},{sql(effective)}::date,
        {sql('explicit_pdf_label' if effective else None)},now(),{pages} FROM resolved
      ON CONFLICT(document_source,source_document_id) DO UPDATE SET
        document_type=EXCLUDED.document_type,document_type_raw=EXCLUDED.document_type_raw,
        filing_date=EXCLUDED.filing_date,filing_date_raw=EXCLUDED.filing_date_raw,
        content_hash=EXCLUDED.content_hash,content_object_storage_key=EXCLUDED.content_object_storage_key,
        source_record_hash=EXCLUDED.source_record_hash,source_ingestion_run_id=EXCLUDED.source_ingestion_run_id,
        effective_date=EXCLUDED.effective_date,effective_date_basis=EXCLUDED.effective_date_basis,
        page_count=EXCLUDED.page_count,stored_at=coalesce(ks_well_document.stored_at,now()),
        parser_status='extracted',last_observed_at=now(),updated_at=now() RETURNING id
    ) SELECT id FROM inserted;
    INSERT INTO ks_document_version(document_id,content_hash,object_storage_key,size_bytes,page_count,source_url,metadata_hash,
       source_received_on,effective_date,effective_date_basis,source_metadata)
    SELECT id,{sql(checksum)},{sql(key)},{len(pdf)},{pages},{sql(item['url'])},{sql(source_hash)},{sql(item['received'])}::date,
       {sql(effective)}::date,{sql('explicit_pdf_label' if effective else None)},{metadata_json} FROM doc
    ON CONFLICT(document_id,content_hash,metadata_hash) DO NOTHING;
    UPDATE ks_well_document d SET current_version_id=v.id FROM ks_document_version v
      WHERE d.id=(SELECT id FROM doc) AND v.document_id=d.id AND v.content_hash={sql(checksum)} AND v.metadata_hash={sql(source_hash)};
    """
    for typ in ['DOCUMENT_ADDED'] + ([event_type] if event_type else []):
        effective_event = item['received'] if typ in ('DOCUMENT_ADDED', 'INTENT_POSTED') else effective
        payload = dict(document_id=None, document_type=kind, title=item['label'], source_url=item['url'],
                       filing_date=item['received'], effective_date=effective,
                       date_basis='KGS_receipt' if typ in ('DOCUMENT_ADDED', 'INTENT_POSTED') else 'effective_date_if_verified',
                       historical_backfill=date.fromisoformat(item['received']) < date.today(), pipeline_version=VERSION)
        statement += f"""
        WITH event AS (
          INSERT INTO ks_event(event_type,dedup_key,occurred_at,published_at,observed_at,payload,source_ingestion_run_id,
            related_well_id,related_county)
          SELECT {sql(typ)},'document:'||d.id,{sql(effective_event)}::date::timestamptz,
            {sql(item['received'])}::date::timestamptz,now(),
            {sql(json.dumps(payload))}::jsonb || jsonb_build_object('document_id',d.id),
            (SELECT id FROM run),w.internal_well_id,w.county FROM doc d CROSS JOIN resolved w
          ON CONFLICT(event_type,dedup_key) DO UPDATE SET occurred_at=EXCLUDED.occurred_at,
            published_at=EXCLUDED.published_at,payload=EXCLUDED.payload RETURNING id
        ) INSERT INTO ks_document_event_link(document_id,event_id,version_id)
          SELECT d.id,e.id,v.id FROM doc d CROSS JOIN event e
          JOIN ks_document_version v ON v.document_id=(SELECT id FROM doc) AND v.content_hash={sql(checksum)} AND v.metadata_hash={sql(source_hash)}
          ON CONFLICT(document_id,event_id) DO UPDATE SET version_id=EXCLUDED.version_id;
        """
    statement += "SELECT json_build_object('document_id',id,'events',(SELECT count(*) FROM ks_document_event_link WHERE document_id=doc.id)) FROM doc; COMMIT;"
    result = query(statement)
    return json.loads(result.splitlines()[-1])


def process(well, storage, since, recheck=False):
    page = fetch(DETAIL + well['kgs_kid'], 4 * 1024 * 1024)
    items = parse_page(page.decode('utf-8', 'strict'), well['kgs_kid'], well['api14'])
    imported = []
    for item in items:
        if item['received'] < str(since):
            continue
        identity = 'kgs:pdf:' + digest(item['url'].encode())
        known = rows("SELECT d.id,d.well_id,d.content_hash,d.filing_date,d.document_type_raw FROM ks_well_document d "
                     "WHERE d.document_source='KGS_DOCUMENTS' AND d.source_document_id=" + sql(identity) +
                     " AND d.current_version_id IS NOT NULL AND d.content_object_storage_key IS NOT NULL")
        if known and known[0]['well_id'] != well['id']:
            raise ValueError('PDF already resolved to a different well')
        # Each source URL is revisited weekly; unchanged daily metadata needs no download.
        if (known and not recheck and known[0]['filing_date'] == item['received']
                and known[0]['document_type_raw'] == item['label']):
            continue
        pdf = fetch(item['url'], MAX_PDF)
        if not pdf.startswith(b'%PDF-') or b'%%EOF' not in pdf[-4096:]:
            raise ValueError('source is not a complete PDF')
        with tempfile.TemporaryDirectory(prefix='ks-pdf-') as temp:
            path = Path(temp) / 'source.pdf'
            path.write_bytes(pdf)
            info = subprocess.run(['pdfinfo', str(path)], capture_output=True, text=True, timeout=30, check=True)
            match = re.search(r'^Pages:\s*(\d+)', info.stdout, re.M)
            if not match or not 1 <= int(match[1]) <= 2000 or re.search(r'^Encrypted:\s+yes', info.stdout, re.M):
                raise ValueError('PDF page count/encryption requires review')
            textfile = Path(temp) / 'text.txt'
            subprocess.run(['pdftotext', '-f', '1', '-l', '4', '-layout', str(path), str(textfile)],
                           capture_output=True, check=True, timeout=60)
            text = textfile.read_text(errors='replace')
            # KGS's exact KID/API link is authoritative even for image-only PDFs.
            # An explicit different API on its cover requires review.
            cover_apis = {api14(m) for m in re.findall(r'\b15-\d{3}-\d{5}(?:-\d{2}-?\d{2})?\b', text)}
            if cover_apis and cover_apis != {well['api14']}:
                raise ValueError('PDF contains other/multiple well APIs')
            effective = effective_date(text, classify(item['label'])[0])
            if effective and effective > item['received']:
                effective = None
            key = storage.store(path, digest(pdf))
            imported.append(publish(well, item, pdf, int(match[1]), key, effective))
    return digest(page), len(items), imported


def main():
    load_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--since', default=os.environ.get('KS_DOCUMENTS_SINCE', '2025-01-01'))
    parser.add_argument('--limit', type=int, default=int(os.environ.get('KS_DOCUMENTS_LIMIT', '2000')))
    parser.add_argument('--kid', action='append', help='Explicit source KID for a targeted normal ingestion run')
    parser.add_argument('--recheck', action='store_true', help='Download and verify unchanged source URLs too')
    args = parser.parse_args()
    since = date.fromisoformat(args.since)
    if since > date.today() or not 1 <= args.limit <= 10000:
        raise ValueError('invalid discovery scope')
    directory = ROOT / 'data/kansas/documents'
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'worker.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        storage = R2()
        storage.call('head-bucket')
        conditions = ('AND w.kgs_kid IN (' + ','.join(sql(k) for k in args.kid) + ')') if args.kid else """
          AND (s.well_id IS NULL OR s.checked_at < now()-interval '7 days'
            OR (s.error IS NOT NULL AND s.checked_at < now()-interval '1 day'))"""
        wells = rows(f"""SELECT w.internal_well_id AS id,w.api14,w.kgs_kid,s.checked_at
          FROM ks_well w JOIN ks_ingestion_run r ON r.id=w.source_ingestion_run_id
          LEFT JOIN ks_document_scan s ON s.well_id=w.internal_well_id
          WHERE NOT w.is_tombstoned AND w.api14 ~ '^15[0-9]{{12}}$' AND w.kgs_kid ~ '^[0-9]+$'
           AND r.status='completed' AND r.row_count>100000 {conditions}
          ORDER BY CASE WHEN greatest(w.permit_date,w.completion_date,w.plug_date)>={sql(since)}::date THEN 0 ELSE 1 END,
            s.checked_at ASC NULLS FIRST,greatest(w.permit_date,w.completion_date,w.plug_date) DESC NULLS LAST,w.api14
          LIMIT {args.limit}""")
        report = dict(started_at=datetime.now(timezone.utc).isoformat(), since=str(since), pipeline_version=VERSION,
                      selected=len(wells), checked=0, imported=0, failed=0, failures=[])
        for well in wells:
            try:
                # A successful scan older than a week forces PDF revision checks.
                result = process(well, storage, since, args.recheck or well['checked_at'] is not None)
                page_hash, count, imported = result
                query(f"""INSERT INTO ks_document_scan(well_id,page_hash,document_count) VALUES
                  ({sql(well['id'])}::uuid,{sql(page_hash)},{count}) ON CONFLICT(well_id) DO UPDATE SET
                  checked_at=now(),page_hash=EXCLUDED.page_hash,document_count=EXCLUDED.document_count,error=NULL,failures=0;""")
                report['checked'] += 1
                report['imported'] += len(imported)
                print(json.dumps(dict(kid=well['kgs_kid'], api14=well['api14'], documents=len(imported))), flush=True)
            except Exception as error:
                message = safe_error(error)
                query(f"""INSERT INTO ks_document_scan(well_id,error,failures) VALUES
                  ({sql(well['id'])}::uuid,{sql(message)},1) ON CONFLICT(well_id) DO UPDATE SET
                  checked_at=now(),error=EXCLUDED.error,failures=ks_document_scan.failures+1;""")
                report['failed'] += 1
                report['failures'].append(dict(kid=well['kgs_kid'], error=message))
            write_json(directory / 'latest-run.json', report)
            time.sleep(float(os.environ.get('KS_DOCUMENTS_DELAY', '0.3')))
        report['completed_at'] = datetime.now(timezone.utc).isoformat()
        write_json(directory / 'latest-run.json', report)
        print(json.dumps(report), flush=True)
        if report['failed']:
            raise RuntimeError('Some well document scans failed; see the private run report')


if __name__ == '__main__':
    main()
