# VMA writer module
#
# Copyright 2013 Red Hat, Inc. and/or its affiliates
#
# Authors:
#   Stefan Hajnoczi <address@hidden>
#
# This work is licensed under the terms of the GNU GPL, version 2 or later.
# See the COPYING file in the top-level directory.

import array
import struct
import hashlib
import uuid
import time

__all__ = ['Writer']

VMA_MAGIC = 0x564d4100
VMA_VERSION = 1
VMA_MAX_CONFIGS = 256
VMA_CLUSTER_SIZE = 65536
VMA_BLOCKS_PER_EXTENT = 59
VMA_EXTENT_MAGIC = 0x564d4145

header_struct = struct.Struct('>II16cQ16cIII')
dev_info_struct = struct.Struct('>IIQQQ')
extent_struct = struct.Struct('>I2xH16c16c')
le16_struct = struct.Struct('<H')
be32_struct = struct.Struct('>I')
be64_struct = struct.Struct('>Q')

class Writer(object):
    def __init__(self, fobj):
        self.fobj = fobj
        self.uuid = uuid.uuid4().bytes
        self.streams = []
        self.blobs = ['\0']
        self.blob_offset = 1
        self.config_names = []
        self.config_data = []
        self.header_written = False
        self.align_bufs = {}
        self.extent = []

    def alloc_blob(self, blob):
        '''Return allocated blob buffer offset'''
        offset = self.blob_offset
        self.blobs.append(le16_struct.pack(len(blob)))
        self.blobs.append(blob)
        self.blob_offset += le16_struct.size + len(blob)
        return offset

    def alloc_blob_str(self, s):
        '''Return allocated blob buffer offset for string'''
        return self.alloc_blob(s + '\0')

    def build_dev_info(self):
        '''Return a buffer with device infos'''
        bufs = ['\0' * dev_info_struct.size]
        for name, size in self.streams:
            name_ptr = self.alloc_blob_str(name)
            buf = dev_info_struct.pack(name_ptr, 0, size, 0, 0)
            bufs.append(buf)
        padding = (255 - len(self.streams)) * dev_info_struct.size
        bufs.append('\0' * padding)
        return ''.join(bufs)

    def build_blob_buffer(self):
        '''Return a buffer with blob data'''
        return ''.join(self.blobs)

    def build_config(self):
        '''Return a buffer with config names and data'''
        bufs = []

        for ptr in self.config_names:
            bufs.append(be32_struct.pack(ptr))
        padding = (VMA_MAX_CONFIGS - len(self.config_names)) * be32_struct.size
        bufs.append('\0' * padding)

        for ptr in self.config_data:
            bufs.append(be32_struct.pack(ptr))
        padding = (VMA_MAX_CONFIGS - len(self.config_data)) * be32_struct.size
        bufs.append('\0' * padding)

        return ''.join(bufs)

    def write_header(self):
        # Build header pieces
        config = self.build_config()
        dev_info = self.build_dev_info()
        blob_buffer = self.build_blob_buffer()

        # Size the header
        blob_buffer_offset = header_struct.size + 1984 + \
                             len(config) + 4 + len(dev_info)
        header_size = blob_buffer_offset + len(blob_buffer)

        # Build header without checksum
        fields = (VMA_MAGIC,
                  VMA_VERSION) + \
                 tuple(self.uuid) + \
                 (int(time.mktime(time.gmtime())),) + \
                 tuple('\0' * 16) + \
                 (blob_buffer_offset,
                  len(blob_buffer),
                  header_size)
        header = header_struct.pack(*fields)

        # Checksum header
        buf = ''.join([header,
                       '\0' * 1984,
                       config,
                       '\0' * 4, # VMAHeader.dev_info is unaligned (vma.h bug)
                       dev_info,
                       blob_buffer])
        digest = hashlib.md5(buf).digest()
        buf = array.array('c', buf) # string does not support assignment
        buf[32:32 + 16] = array.array('c', digest)

        self.fobj.write(buf)

    def add_config(self, name, data):
        name_ptr = self.alloc_blob_str(name)
        data_ptr = self.alloc_blob(data)
        self.config_names.append(name_ptr)
        self.config_data.append(name_ptr)

    def add_stream(self, name, size):
        self.streams.append((name, size))
        return len(self.streams)

    def build_blockinfo(self):
        '''Return a blockinfo buffer for the current extent'''
        bufs = []
        for stream_id, offset, _ in self.extent:
            buf = be64_struct.pack(0xffff000000000000 | \
                                   (stream_id << 32)  | \
                                   offset // VMA_CLUSTER_SIZE)
            bufs.append(buf)
        padding = (VMA_BLOCKS_PER_EXTENT - len(self.extent)) * be64_struct.size
        bufs.append('\0' * padding)
        return ''.join(bufs)

    def write_extent(self):
        blockinfo = self.build_blockinfo()
        block_count = len(self.extent) * (VMA_CLUSTER_SIZE // 4096)

        # Build header without checksum
        fields = (VMA_EXTENT_MAGIC,
                  block_count) + \
                 tuple(self.uuid) + \
                 tuple('\0' * 16)
        header = extent_struct.pack(*fields)

        # Checksum header
        buf = ''.join([header, blockinfo])
        digest = hashlib.md5(buf).digest()
        buf = array.array('c', buf) # string does not support assignment
        buf[24:24 + 16] = array.array('c', digest)

        self.fobj.write(buf)
        for _, _, data in self.extent:
            self.fobj.write(data)

        self.extent = []

    def append_cluster(self, stream_id, offset, data):
        '''Append one cluster to the current extent'''
        self.extent.append((stream_id, offset, data))
        if len(self.extent) == VMA_BLOCKS_PER_EXTENT:
            self.write_extent()

    def align_write(self, stream_id, offset, data):
        '''Buffer writes whose length is not cluster-aligned (vmstate)'''
        # Fast path for aligned writes
        mod = len(data) % VMA_CLUSTER_SIZE
        if stream_id not in self.align_bufs and mod == 0:
            return False, offset, data

        # Add data to buffer
        bufs, start, total = self.align_bufs.get(stream_id, ([], offset, 0))
        assert start + total == offset # must be sequential
        bufs.append(data)
        total += len(data)
        self.align_bufs[stream_id] = (bufs, start, total)

        # Stop if we don't have a cluster yet
        if total < VMA_CLUSTER_SIZE:
            return True, None, None

        # Take as many clusters as possible
        end = (total // VMA_CLUSTER_SIZE) * VMA_CLUSTER_SIZE
        aligned = []
        nbytes = 0
        while nbytes < end:
            buf = bufs.pop(0)
            aligned.append(buf)
            nbytes += len(buf)
        if nbytes > end:
            buf = aligned[-1]
            keep = end - (nbytes - len(buf))
            left, right = buf[:keep], buf[keep:]
            aligned[-1] = left
            bufs.insert(0, right)
        self.align_bufs[stream_id] = (bufs, start + end, total - end)
        return False, start, ''.join(aligned)

    def write(self, stream_id, offset, data):
        if not self.header_written:
            self.write_header()
            self.header_written = True

        need_more, offset, data = self.align_write(stream_id, offset, data)
        if need_more:
            return

        for i in range(len(data) // VMA_CLUSTER_SIZE):
            self.append_cluster(stream_id, offset, data[:VMA_CLUSTER_SIZE])
            data = data[VMA_CLUSTER_SIZE:]
            offset += VMA_CLUSTER_SIZE

    def close(self):
        # Flush unaligned data
        for stream_id in self.align_bufs.keys():
            bufs, start, total = self.align_bufs[stream_id]
            assert total < VMA_CLUSTER_SIZE
            padding = VMA_CLUSTER_SIZE - total
            bufs.append('\0' * padding)
            self.append_cluster(stream_id, start, ''.join(bufs))
        self.align_bufs = {}

        # Write final extent, if necessary
        if self.extent:
            self.write_extent()
