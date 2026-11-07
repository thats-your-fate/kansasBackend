"""Native ingestion runtime. Secrets stay in the environment, never argv/logs."""
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import urlsplit, unquote, parse_qs

ROOT = Path(__file__).resolve().parents[1]


def load_env(path=ROOT / '.env'):
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, value = line.removeprefix('export ').split('=', 1)
            os.environ.setdefault(key, value.strip().strip('\"\''))


def sql(value):
    if value is None:
        return 'NULL'
    return "'" + str(value).replace("'", "''") + "'"


def pg_env():
    uri = urlsplit(os.environ.get('DATABASE_URL') or os.environ['PG_CONNECTION'])
    env = dict(os.environ, PGHOST=uri.hostname, PGPORT=str(uri.port or 5432),
               PGDATABASE=unquote(uri.path.lstrip('/')), PGUSER=unquote(uri.username or ''),
               PGPASSWORD=unquote(uri.password or ''), PGCONNECT_TIMEOUT='15')
    for key in ['sslmode', 'sslrootcert']:
        if key in parse_qs(uri.query):
            env['PG' + key.upper()] = parse_qs(uri.query)[key][0]
    return env


def query(statement, timeout=120):
    result = subprocess.run(['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1'],
                            input="SET standard_conforming_strings=on;\n" + statement,
                            env=pg_env(), capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        # PostgreSQL can include submitted values in error detail. Keep those private.
        raise RuntimeError('database_operation_failed: ' +
                           next((s for s in result.stderr.splitlines() if 'ERROR:' in s), 'psql failed')[:180])
    return result.stdout.strip()


def rows(statement, timeout=120):
    return json.loads(query("SELECT coalesce(json_agg(t),'[]'::json) FROM (" + statement + ')t;', timeout))


def cli(*args, timeout=7200):
    env = dict(os.environ, KS_DB_MAX_CONNECTIONS='2', RUST_LOG='warn')
    result = subprocess.run([str(ROOT / 'target/release/ks_ingest'), *map(str, args)],
                            cwd=ROOT, env=env, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError('ks_ingest failed: ' + args[0])
    return result.stdout


def safe_error(error):
    text = re.sub(r'(?:postgres(?:ql)?|https?|socks5h?)://\S+', '[redacted URL]', str(error))
    return text[-300:]


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)
