#!/usr/bin/env python
#
# Live backup tool for QEMU
#
# Copyright 2013 Red Hat, Inc. and/or its affiliates
#
# Authors:
#   Stefan Hajnoczi <address@hidden>
#
# This work is licensed under the terms of the GNU GPL, version 2 or later.
# See the COPYING file in the top-level directory.

import sys; sys.path.append('QMP')
import qmp
import subprocess

MIGRATE_PATH = '/tmp/migrate.sock'
NBD_PATH = '/tmp/nbd.sock'

def find_backup_drives(mon):
    '''Return list of devices that should be backed up'''
    result = []
    for info in mon.command('query-block'):
        # Skip empty drives
        if 'inserted' not in info:
            continue

        # Skip read-only drives
        if info['inserted']['ro']:
            continue

        result.append((info['device'], info['inserted']['virtual_size']))
    return result

def spawn_writer_process(filename, drives):
    '''Return Popen instance for vma-writer.py process'''
    args = ['python', 'vma-writer.py',
            '--output', filename,
            '--incoming', MIGRATE_PATH,
            '--nbd', NBD_PATH]
    for name, size in drives:
        args.extend(('--drive', 'name=%s,size=%s' % (name, size)))
    writer = subprocess.Popen(args, stdout=subprocess.PIPE)
    writer.stdout.readline() # Wait for "Ready"
    return writer

def main(args):
    mon = qmp.QEMUMonitorProtocol(args[1])
    mon.connect()

    drives = find_backup_drives(mon)
    writer = spawn_writer_process(args[2], drives)

    sys.stderr.write('Running migration...\n')
    mon.command('migrate', uri='unix:' + MIGRATE_PATH)
    while True:
        evt = mon.pull_event(wait=True)
        if evt['event'] == 'STOP':
            break

    sys.stderr.write('Running block-backup...\n')
    for name, _ in drives:
        mon.command('block-backup',
                    device=name,
                    target='nbd+unix:///%s?socket=%s' % (name, NBD_PATH),
                    format='raw',
                    mode='existing')
    mon.command('cont')
    pending_drives = set(name for name, _ in drives)
    while pending_drives:
        evt = mon.pull_event(wait=True)
        if evt['event'] == 'BLOCK_JOB_COMPLETED':
            name = evt['data']['device']
            sys.stderr.write('Finished device "%s"\n' % name)
            pending_drives.discard(name)
    sys.stderr.write('Backup complete, terminating writer process\n')

    # Wait for writer process to terminate and print its output
    sys.stdout.write(writer.communicate()[0])

    mon.close()

if __name__ == '__main__':
    main(sys.argv)
