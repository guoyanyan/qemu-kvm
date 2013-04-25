#!/usr/bin/env python
#
# VMA backup archive writer
#
# Copyright 2013 Red Hat, Inc. and/or its affiliates
#
# Authors:
#   Stefan Hajnoczi <address@hidden>
#
# This work is licensed under the terms of the GNU GPL, version 2 or later.
# See the COPYING file in the top-level directory.

import argparse
import threading
import Queue
import socket
import sys
import os
import nbd
import vma

WRITER_OP_WRITE = 0
WRITER_OP_STOP = 1

def setup_listen_sockets(paths):
    '''Return list of listening UNIX domain sockets'''
    socks = []
    for path in paths:
        s = socket.socket(socket.AF_UNIX)
        try:
            os.unlink(path)
        except OSError:
            pass
        s.bind(path)
        s.listen(0)
        socks.append(s)
    return socks

def vma_writer_thread(writer, queue):
    while True:
        cmd = queue.get()
        if cmd[0] == WRITER_OP_STOP:
            break
        assert cmd[0] == WRITER_OP_WRITE
        _, stream_id, offset, data = cmd
        writer.write(stream_id, offset, data)
    writer.close()

def setup_vma_writer(filename, drives):
    vma_file = open(filename, 'wb')
    writer = vma.Writer(vma_file)

    vmstate_id = writer.add_stream('vmstate', 1)
    for drive in drives:
        drive['stream_id'] = writer.add_stream(drive['name'], int(drive['size']))

    queue = Queue.Queue()
    t = threading.Thread(target=vma_writer_thread, args=(writer, queue))
    t.start()
    return queue, vmstate_id

def consume_migration(sock, queue, stream_id):
    '''Write vmstate data into archive'''
    conn, _ = sock.accept()
    sock.close()

    offset = 0
    while True:
        buf = conn.recv(256 * 1024)
        if len(buf) == 0:
            break
        queue.put((WRITER_OP_WRITE, stream_id, offset, buf))
        offset += len(buf)

    conn.close()

def parse_option_list(s):
    return dict(kv.split('=') for kv in s.split(','))

class NBDHandler(nbd.ExportHandler):
    def __init__(self, size, queue, stream_id):
        self._size = size
        self.queue = queue
        self.stream_id = stream_id

    def write(self, offset, data):
        self.queue.put((WRITER_OP_WRITE, self.stream_id, offset, data))

    def size(self):
        return self._size

def consume_nbd(sock, drives):
    server = nbd.Server(sock)
    for drive in drives:
        server.add_export(drive['name'],
                NBDHandler(int(drive['size']), queue, drive['stream_id']))
    server.run()

parser = argparse.ArgumentParser(description='VMA backup archive writer')
parser.add_argument('--incoming',
                    help='UNIX domain socket for incoming migration',
                    required=True)
parser.add_argument('--nbd',
                    help='UNIX domain socket for NBD server',
                    required=True)
parser.add_argument('--drive',
                    help='Device name of drive to back up',
                    action='append',
                    default=[])
parser.add_argument('--output',
                    help='Backup archive filename',
                    required=True)

args = parser.parse_args()
drives = [parse_option_list(opts) for opts in args.drive]
queue, vmstate_id = setup_vma_writer(args.output, drives)
socks = setup_listen_sockets((args.incoming, args.nbd))

# Let parent process know the sockets are listening
sys.stdout.write('Ready\n')
sys.stdout.flush()

consume_migration(socks[0], queue, vmstate_id)
consume_nbd(socks[1], drives)

queue.put((WRITER_OP_STOP,))
