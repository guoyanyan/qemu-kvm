/*
 *  TCP Proxy block backend for QEMU.
 * 
 *  Copyright (C) 2013 Huawei Inc., Hao Luo <brian.luohao@huawei.com>
 *  
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 * 
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *  
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 *  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */

#include "qemu-common.h"
#include "block/block_int.h"
#include "qemu/sockets.h"
#include "qemu/uri.h"
#include "qapi/qmp/qint.h"

#define DEBUG_RFD     1

#define DPRINTF(fmt, ...)                           \
    do {                                            \
        if (DEBUG_RFD) {                            \
            fprintf(stderr, "tcp: %-15s " fmt "\n", \
                    __func__, ##__VA_ARGS__);       \
        }                                           \
    } while (0)


typedef struct BDRVTCPState {
    CoMutex lock;
    int     sock;
    int64_t offset;
    char    *hostport;
} BDRVTCPState;


static void tcp_state_init(BDRVTCPState *s)
{
    memset(s, 0, sizeof *s);
    s->sock = -1;
    s->offset = -1;
    qemu_co_mutex_init(&s->lock);
}

static void tcp_state_free(BDRVTCPState *s)
{
    g_free(s->hostport);

    if (s->sock >= 0) {
        close(s->sock);
    }
}


static void tcp_parse_filename(const char *filename, QDict *options,
                               Error **errp) {
    URI *uri = NULL;

    if (strcmp(uri->scheme, "tcp") != 0) {
        error_setg(errp, "URI scheme must be 'tcp'");
        goto err;
    }

    if (!uri->server || strcmp(uri->server, "") == 0) {
        error_setg(errp, "missing hostname in URI");
        goto err;
    }

    if (!uri->path || strcmp(uri->path, "") == 0) {
        error_setg(errp, "missing remote path in URI");
        goto err;
    }

    qdict_put(options, "host", qstring_from_str(uri->server));

    if (uri->port) {
        qdict_put(options, "port", qint_from_int(uri->port));
    }

    qdict_put(options, "path", qstring_from_str(uri->path));

err:
    if (uri) {
      uri_free(uri);
    }
}


static int tcp_file_open(BlockDriverState *bs, const char *filename,
                         QDict *options, int bdrv_flags)  {

    const char *host, *path;
    int port, ret;
    Error *err = NULL;
    BDRVTCPState *s = bs->opaque;

    tcp_state_init(s);

    host = qdict_get_str(options, "host");

    if (qdict_haskey(options, "port")) {
        port = qdict_get_int(options, "port");
    } else {
        port = 2222;
    }

    g_free(s->hostport);
    s->hostport = g_strdup_printf("%s:%d", host, port);

    /* Open the socket and connect. */
    s->sock = inet_connect(s->hostport, &err);
    if (err != NULL) {
        ret = -errno;
        qerror_report_err(err);
        error_free(err);
        return ret;
    }

    qemu_set_nonblock(s->sock);

    path = qdict_get_str(options, "path");

    /* Delete the options we've used; any not deleted will cause the
     * block layer to give an error about unused options.
     */
    qdict_del(options, "host");
    qdict_del(options, "port");
    qdict_del(options, "path");

    return 0;
}

static void tcp_close(BlockDriverState *bs)
{
    BDRVTCPState *s = bs->opaque;

    tcp_state_free(s);
}

/*
 * stub functions
 */
static void tcp_parse_filename_stub(const char *filename, QDict *options,
                               Error **errp) {
    DPRINTF("parsing tcp block device uri.");
    return;
}

static int tcp_create_stub(const char *filename, QEMUOptionParameter *options) {
    DPRINTF("creating tcp block device.");
    return 0;
}

static int64_t tcp_getlength_stub(BlockDriverState *bs) {
    DPRINTF("get length of tcp block device.");
    return 1024 * 1024 * 1024 / BDRV_SECTOR_SIZE;
}

static int tcp_open_stub(BlockDriverState *bs, const char *filename,
                         QDict *options, int bdrv_flags)  {

    DPRINTF("opening tcp block device.");
    BDRVTCPState *s = bs->opaque;

    tcp_state_init(s);
    return 0;
}


static void tcp_close_stub(BlockDriverState *bs) {
    DPRINTF("closing tcp block device.");
    BDRVTCPState *s = bs->opaque;

    tcp_state_free(s);
    return;
}


static coroutine_fn int tcp_co_readv_stub(BlockDriverState *bs,
                                     int64_t sector_num,
                                     int nb_sectors, QEMUIOVector *qiov) {
    DPRINTF("sector num: 0x%" PRIx64 ",nsectors: %d\n", sector_num, nb_sectors);
    
    return 0;
}

static coroutine_fn int tcp_co_writev_stub(BlockDriverState *bs,
                                      int64_t sector_num,
                                      int nb_sectors, QEMUIOVector *qiov) {
    
    
    DPRINTF("sector num: 0x%" PRIx64 ",nsectors: %d\n", sector_num, nb_sectors);
    
    return 0;
}

static coroutine_fn int tcp_co_flush_stub(BlockDriverState *bs)
{
    //DPRINTF("flushing tcp block device.");

    return 0;
}


static BlockDriver bdrv_tcp = {
    .format_name                  = "tcp",
    .protocol_name                = "tcp",
    .instance_size                = sizeof(BDRVTCPState),
    .bdrv_parse_filename          = tcp_parse_filename_stub,
    .bdrv_create                  = tcp_create_stub,
    .bdrv_file_open               = tcp_open_stub,
    .bdrv_getlength               = tcp_getlength_stub,
    .bdrv_close                   = tcp_close_stub,
    .bdrv_co_writev               = tcp_co_writev_stub,
    .bdrv_co_readv                = tcp_co_readv_stub,
    .bdrv_co_flush_to_disk        = tcp_co_flush_stub,
};

static void bdrv_tcp_init(void)
{
    DPRINTF("initializing tcp block device.");
    bdrv_register(&bdrv_tcp);
}

block_init(bdrv_tcp_init);

