#!/usr/bin/env python3
"""Restricted SSH entry point for the isolated Kansas exporter.

Installed as the forced command of the Future Wells Co sync key once Kansas
runs under its dedicated runtime account. It accepts exactly the commands the
original helper accepted from SSH and hands them to the fixed root-owned
isolation helper, which runs them as fw-ks-runtime inside the sealed release.
It never runs a worker, a shell or any other command itself.
"""
import os
import shlex
import sys
import uuid

HELPER = '/usr/local/libexec/seo_ks_isolation.py'
SUDO = '/usr/bin/sudo'


def identifier(value):
    if str(uuid.UUID(value)) != value:
        raise ValueError('canonical UUID required')
    return value


def command(environ):
    if 'SSH_ORIGINAL_COMMAND' not in environ:
        raise ValueError('forced-command entry point only')
    args = shlex.split(environ['SSH_ORIGINAL_COMMAND'])
    if args in (['export'], ['preflight']):
        return args
    if len(args) == 2 and args[0] in ('describe', 'archive', 'ack'):
        return [args[0], identifier(args[1])]
    raise ValueError('allowed commands: preflight, export, describe UUID, archive UUID, ack UUID')


def main():
    args = command(os.environ)
    sys.stdout.flush()
    os.execv(SUDO, [SUDO, '-n', HELPER, '--sync', *args])


if __name__ == '__main__':
    main()
