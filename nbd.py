# NBD server module
#
# Copyright 2013 Red Hat, Inc. and/or its affiliates
#
# Authors:
#   Stefan Hajnoczi <address@hidden>
#
# This work is licensed under the terms of the GNU GPL, version 2 or later.
# See the COPYING file in the top-level directory.

import struct
import collections
import threading

__all__ = ['ExportHandler', 'Server']

NBD_CMD_WRITE = 1
NBD_CMD_DISC = 2
NBD_REQUEST_MAGIC = 0x25609513
NBD_REPLY_MAGIC = 0x67446698
NBD_PASSWD = 0x4e42444d41474943
NBD_OPTS_MAGIC = 0x49484156454F5054
NBD_OPT_EXPORT_NAME = 1 << 0

neg1_struct = struct.Struct('>QQH')
export_tuple = collections.namedtuple('Export', 'reserved magic opt len')
export_struct = struct.Struct('>IQII')
neg2_struct = struct.Struct('>QH124x')
request_tuple = collections.namedtuple('Request', 'magic type handle from_ len')
request_struct = struct.Struct('>IIQQI')
reply_struct = struct.Struct('>IIQ')

def recvall(sock, bufsize):
    received = 0
    chunks = []
    while received < bufsize:
        chunk = sock.recv(bufsize - received)
        if len(chunk) == 0:
            raise Exception('unexpected disconnect')
        chunks.append(chunk)
        received += len(chunk)
    return ''.join(chunks)

class ExportHandler(object):
    def write(self, offset, data):
        pass

    def size(self):
        return 0

def negotiate(conn, exports):
    '''Negotiate export with client'''
    # Send negotiation part 1
    buf = neg1_struct.pack(NBD_PASSWD, NBD_OPTS_MAGIC, 0)
    conn.sendall(buf)

    # Receive export option
    buf = recvall(conn, export_struct.size)
    export = export_tuple._make(export_struct.unpack(buf))
    assert export.magic == NBD_OPTS_MAGIC
    assert export.opt == NBD_OPT_EXPORT_NAME
    name = recvall(conn, export.len)

    if name not in exports:
        print 'name "%s" not in exports' % name
        return None
    handler = exports[name]

    # Send negotiation part 2
    buf = neg2_struct.pack(handler.size(), 0)
    conn.sendall(buf)
    return handler

def read_request(conn):
    '''Parse NBD request from client'''
    buf = recvall(conn, request_struct.size)
    req = request_tuple._make(request_struct.unpack(buf))
    assert req.magic == NBD_REQUEST_MAGIC
    return req

def write_reply(conn, error, handle):
    buf = reply_struct.pack(NBD_REPLY_MAGIC, error, handle)
    conn.sendall(buf)

def server_connection_thread(conn, exports):
    handler = negotiate(conn, exports)
    if handler is None:
        conn.close()
        return

    while True:
        req = read_request(conn)
        if req.type == NBD_CMD_WRITE:
            # Reply immediately, don't propagate internal errors to client
            write_reply(conn, 0, req.handle)

            data = recvall(conn, req.len)
            handler.write(req.from_, data)
        elif req.type == NBD_CMD_DISC:
            break
        else:
            print 'unrecognized command type %#02x' % req.type
            break
    conn.close()

class Server(object):
    def __init__(self, sock):
        self.sock = sock
        self.exports = {}

    def add_export(self, name, handler):
        self.exports[name] = handler

    def run(self):
        threads = []
        for i in range(len(self.exports)):
            conn, _ = self.sock.accept()
            t = threading.Thread(target=server_connection_thread,
                                 args=(conn, self.exports))
            t.daemon = True
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
