#!/usr/bin/env python3
"""Create or rotate the Kansas admin account without storing a plaintext password."""
import getpass
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys

path = Path.home() / '.config/future-wells/kansas-admin.json'
username = sys.argv[1].strip().lower() if len(sys.argv) == 2 else 'yaro'
password = getpass.getpass('Kansas admin password: ')
if len(password) < 8 or len(password) > 256:
    raise ValueError('password must contain 8–256 characters')
salt = secrets.token_hex(32)
value = {'username': username, 'role': 'superadmin', 'salt': salt,
         'password_hash': hashlib.scrypt(password.encode(), salt=salt.encode(), n=16384, r=8, p=1, dklen=64).hex(),
         'session_key': secrets.token_hex(48)}
path.parent.mkdir(parents=True, exist_ok=True)
temporary = path.with_suffix('.tmp')
fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, 'w') as output:
    json.dump(value, output)
os.chmod(temporary, 0o600)
temporary.replace(path)
print('Kansas superadmin created:', username)
