#!/usr/bin/env python3
"""Native current-source refresh and June-2025-forward production publication."""
import argparse
from datetime import date, datetime, timezone
import fcntl
import json
import os

from kansas_common import ROOT, cli, load_env, query, rows, sql, write_json


def refresh_production(since):
    cli('snapshot-production', '--current')
    runs = rows("""SELECT DISTINCT ON (r.source_url) r.id,r.source_url,r.row_count
      FROM ks_ingestion_run r JOIN ks_source s ON s.id=r.source_id
      WHERE r.status='completed' AND s.code IN ('KGS_PRODUCTION_OIL','KGS_PRODUCTION_GAS')
        AND r.source_url IN ('https://www.kgs.ku.edu/PRS/Ora_Archive/oil_leases_2020_present.zip',
                            'https://www.kgs.ku.edu/PRS/Ora_Archive/gas_leases_2020_present.zip')
      ORDER BY r.source_url,r.started_at DESC""")
    if len(runs) != 2:
        raise RuntimeError('both completed current oil and gas source partitions required')
    results = []
    for run in runs:
        product = 'oil' if '/oil_' in run['source_url'] else 'gas'
        source = rows(f"""SELECT year,month,count(*) AS rows,sum(production_volume::numeric) AS volume
          FROM ks_lease_production_raw_snapshot WHERE ingestion_run_id={sql(run['id'])}::uuid
            AND rejection_reason IS NULL AND month BETWEEN 1 AND 12
            AND (year,month)>=(extract(year FROM {sql(since)}::date)::integer,extract(month FROM {sql(since)}::date)::integer)
          GROUP BY year,month ORDER BY year,month""", timeout=300)
        previous = rows(f"""SELECT year,month,count(*) AS rows FROM ks_lease_production_month p
          JOIN ks_ingestion_run r ON r.id=p.source_ingestion_run_id
          WHERE r.source_url={sql(run['source_url'])} AND p.record_kind='monthly'
            AND make_date(year,month,1)>={sql(since)}::date GROUP BY year,month""")
        if not source or sum(x['rows'] for x in source) < 10000:
            raise RuntimeError('refusing empty/test-sized production publication')
        source_counts = {(x['year'], x['month']): x['rows'] for x in source}
        if any(source_counts.get((x['year'], x['month']), 0) < x['rows'] * .9 for x in previous):
            raise RuntimeError('source month disappeared or lost over 10% of records; prior publication retained')
        cli('mark-run-backfill', run['id'])
        cli('materialize-production', run['id'], '--since', str(since))
        actual = rows(f"""SELECT year,month,count(*) AS rows,sum(production_volume::numeric) AS volume
          FROM ks_lease_production_month WHERE source_ingestion_run_id={sql(run['id'])}::uuid
            AND record_kind='monthly' AND make_date(year,month,1)>={sql(since)}::date
          GROUP BY year,month ORDER BY year,month""")
        if actual != source:
            raise RuntimeError('production publication does not reconcile to staged monthly source counts/volumes')
        results.append(dict(product=product, run_id=run['id'], months=actual))
    return results


def refresh_wells():
    output = cli('snapshot-wells', '--zip', timeout=14400)
    import re
    match = re.search(r'run_id:\s*([a-f0-9-]{36})', output)
    if not match:
        raise RuntimeError('snapshot did not return a run identifier')
    run_id = match[1]
    checks = rows(f"""SELECT row_count,status FROM ks_ingestion_run
      WHERE id={sql(run_id)}::uuid AND source_url='https://www.kgs.ku.edu/PRS/Ora_Archive/ks_wells.zip'""")
    if len(checks) != 1 or checks[0]['status'] != 'completed' or checks[0]['row_count'] < 400000:
        raise RuntimeError('refusing incomplete well inventory publication')
    cli('mark-run-backfill', run_id)
    result = cli('materialize-wells', run_id, timeout=21600)
    cli('repair-well-mappings')
    # Historical source events remain historical; new document events carry their
    # own receipt/effective dates and evidence links.
    cli('backfill-well-history', timeout=1800)
    return dict(run_id=run_id, result=result)


def main():
    load_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dataset', choices=['production', 'wells'])
    parser.add_argument('--since', default=os.environ.get('KS_PRODUCTION_SINCE', '2025-06-01'))
    args = parser.parse_args()
    since = date.fromisoformat(args.since)
    if since.day != 1 or since.year < 1990 or since > date.today():
        raise ValueError('invalid monthly production floor')
    directory = ROOT / 'data/kansas' / args.dataset
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'worker.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        report = dict(started_at=datetime.now(timezone.utc).isoformat(), since=str(since))
        report['result'] = refresh_production(since) if args.dataset == 'production' else refresh_wells()
        report['completed_at'] = datetime.now(timezone.utc).isoformat()
        write_json(directory / 'last-success.json', report)
        print(json.dumps(report), flush=True)


if __name__ == '__main__':
    main()
