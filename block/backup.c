/*
 * QEMU backup
 *
 * Copyright (C) 2013 Proxmox Server Solutions
 *
 * Authors:
 *  Dietmar Maurer (address@hidden)
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 *
 */

#include <stdio.h>
#include <errno.h>
#include <unistd.h>

#include "block/block.h"
#include "block/block_int.h"
#include "block/blockjob.h"
#include "qemu/ratelimit.h"

#define DEBUG_BACKUP 0

#define DPRINTF(fmt, ...) \
    do { \
        if (DEBUG_BACKUP) { \
            fprintf(stderr, "backup: " fmt, ## __VA_ARGS__); \
        } \
    } while (0)

#define BACKUP_CLUSTER_BITS 16
#define BACKUP_CLUSTER_SIZE (1 << BACKUP_CLUSTER_BITS)
#define BACKUP_BLOCKS_PER_CLUSTER (BACKUP_CLUSTER_SIZE / BDRV_SECTOR_SIZE)

#define SLICE_TIME 100000000ULL /* ns */

typedef struct CowRequest {
    int64_t start;
    int64_t end;
    QLIST_ENTRY(CowRequest) list;
    CoQueue wait_queue; /* coroutines blocked on this request */
} CowRequest;

typedef struct BackupBlockJob {
    BlockJob common;
    BlockDriverState *target;
    RateLimit limit;
    CoRwlock flush_rwlock;
    uint64_t sectors_read;
    HBitmap *bitmap;
    QLIST_HEAD(, CowRequest) inflight_reqs;
} BackupBlockJob;

/* See if in-flight requests overlap and wait for them to complete */
static void coroutine_fn wait_for_overlapping_requests(BackupBlockJob *job,
                                                       int64_t start,
                                                       int64_t end)
{
    CowRequest *req;
    bool retry;

    do {
        retry = false;
        QLIST_FOREACH(req, &job->inflight_reqs, list) {
            if (end > req->start && start < req->end) {
                qemu_co_queue_wait(&req->wait_queue);
                retry = true;
                break;
            }
        }
    } while (retry);
}

/* Keep track of an in-flight request */
static void cow_request_begin(CowRequest *req, BackupBlockJob *job,
                                     int64_t start, int64_t end)
{
    req->start = start;
    req->end = end;
    qemu_co_queue_init(&req->wait_queue);
    QLIST_INSERT_HEAD(&job->inflight_reqs, req, list);
}

/* Forget about a completed request */
static void cow_request_end(CowRequest *req)
{
    QLIST_REMOVE(req, list);
    qemu_co_queue_restart_all(&req->wait_queue);
}

static int coroutine_fn backup_do_cow(BlockDriverState *bs,
                                      int64_t sector_num, int nb_sectors)
{
    BackupBlockJob *job = (BackupBlockJob *)bs->job;
    CowRequest cow_request;
    struct iovec iov;
    QEMUIOVector bounce_qiov;
    void *bounce_buffer = NULL;
    int ret = 0;
    int64_t start, end;

    qemu_co_rwlock_rdlock(&job->flush_rwlock);

    start = sector_num / BACKUP_BLOCKS_PER_CLUSTER;
    end = DIV_ROUND_UP(sector_num + nb_sectors, BACKUP_BLOCKS_PER_CLUSTER);

    DPRINTF("brdv_co_backup_cow enter %s C%" PRId64 " %" PRId64 " %d\n",
            bdrv_get_device_name(bs), start, sector_num, nb_sectors);

    wait_for_overlapping_requests(job, start, end);
    cow_request_begin(&cow_request, job, start, end);

    for (; start < end; start++) {
        if (hbitmap_get(job->bitmap, start)) {
            DPRINTF("brdv_co_backup_cow skip C%" PRId64 "\n", start);
            continue; /* already copied */
        }

        /* immediately set bitmap (avoid coroutine race) */
        hbitmap_set(job->bitmap, start, 1);

        DPRINTF("brdv_co_backup_cow C%" PRId64 "\n", start);

        if (!bounce_buffer) {
            iov.iov_len = BACKUP_CLUSTER_SIZE;
            iov.iov_base = bounce_buffer = qemu_blockalign(bs, iov.iov_len);
            qemu_iovec_init_external(&bounce_qiov, &iov, 1);
        }

        ret = bdrv_co_readv(bs, start * BACKUP_BLOCKS_PER_CLUSTER,
                            BACKUP_BLOCKS_PER_CLUSTER,
                            &bounce_qiov);
        if (ret < 0) {
            DPRINTF("brdv_co_backup_cow bdrv_read C%" PRId64 " failed\n",
                    start);
            goto out;
        }

        job->sectors_read += BACKUP_BLOCKS_PER_CLUSTER;

        if (!buffer_is_zero(bounce_buffer, BACKUP_CLUSTER_SIZE)) {
            ret = bdrv_co_writev(job->target, start * BACKUP_BLOCKS_PER_CLUSTER,
                                 BACKUP_BLOCKS_PER_CLUSTER,
                                 &bounce_qiov);
            if (ret < 0) {
                DPRINTF("brdv_co_backup_cow dump_cluster_cb C%" PRId64
                        " failed\n", start);
                goto out;
            }
        }

        DPRINTF("brdv_co_backup_cow done C%" PRId64 "\n", start);
    }

out:
    if (bounce_buffer) {
        qemu_vfree(bounce_buffer);
    }

    cow_request_end(&cow_request);

    qemu_co_rwlock_unlock(&job->flush_rwlock);

    return ret;
}

static void coroutine_fn backup_before_write_notify(Notifier *notifier,
                                                    void *opaque)
{
    BdrvTrackedRequest *req = opaque;
    backup_do_cow(req->bs, req->sector_num, req->nb_sectors);
}

static void backup_set_speed(BlockJob *job, int64_t speed, Error **errp)
{
    BackupBlockJob *s = container_of(job, BackupBlockJob, common);

    if (speed < 0) {
        error_set(errp, QERR_INVALID_PARAMETER, "speed");
        return;
    }
    ratelimit_set_speed(&s->limit, speed / BDRV_SECTOR_SIZE, SLICE_TIME);
}

static BlockJobType backup_job_type = {
    .instance_size = sizeof(BackupBlockJob),
    .job_type = "backup",
    .set_speed = backup_set_speed,
};

static void coroutine_fn backup_run(void *opaque)
{
    BackupBlockJob *job = opaque;
    BlockDriverState *bs = job->common.bs;
    Notifier before_write = {
        .notify = backup_before_write_notify,
    };
    int64_t start, end;
    int ret = 0;

    QLIST_INIT(&job->inflight_reqs);
    qemu_co_rwlock_init(&job->flush_rwlock);

    start = 0;
    end = DIV_ROUND_UP(bdrv_getlength(bs) / BDRV_SECTOR_SIZE,
                       BACKUP_BLOCKS_PER_CLUSTER);

    job->bitmap = hbitmap_alloc(end, 0);

    bdrv_add_before_write_notifier(bs, &before_write);

    DPRINTF("backup_run start %s %" PRId64 " %" PRId64 "\n",
            bdrv_get_device_name(bs), start, end);

    for (; start < end; start++) {
        if (block_job_is_cancelled(&job->common)) {
            break;
        }

        /* we need to yield so that qemu_aio_flush() returns.
         * (without, VM does not reboot)
         */
        if (job->common.speed) {
            uint64_t delay_ns = ratelimit_calculate_delay(
                &job->limit, job->sectors_read);
            job->sectors_read = 0;
            block_job_sleep_ns(&job->common, rt_clock, delay_ns);
        } else {
            block_job_sleep_ns(&job->common, rt_clock, 0);
        }

        if (block_job_is_cancelled(&job->common)) {
            break;
        }

        DPRINTF("backup_run loop C%" PRId64 "\n", start);

        ret = backup_do_cow(bs, start * BACKUP_BLOCKS_PER_CLUSTER, 1);
        if (ret < 0) {
            break;
        }

        /* Publish progress */
        job->common.offset += BACKUP_CLUSTER_SIZE;
    }

    notifier_remove(&before_write);

    /* wait until pending backup_do_cow() calls have completed */
    qemu_co_rwlock_wrlock(&job->flush_rwlock);
    qemu_co_rwlock_unlock(&job->flush_rwlock);

    hbitmap_free(job->bitmap);

    bdrv_delete(job->target);

    DPRINTF("backup_run complete %d\n", ret);
    block_job_completed(&job->common, ret);
}

void backup_start(BlockDriverState *bs, BlockDriverState *target,
                  int64_t speed,
                  BlockDriverCompletionFunc *cb, void *opaque,
                  Error **errp)
{
    assert(bs);
    assert(target);
    assert(cb);

    DPRINTF("backup_start %s\n", bdrv_get_device_name(bs));

    BackupBlockJob *job = block_job_create(&backup_job_type, bs, speed,
                                           cb, opaque, errp);
    if (!job) {
        return;
    }

    job->target = target;
    job->common.len = bdrv_getlength(bs);
    job->common.co = qemu_coroutine_create(backup_run);
    qemu_coroutine_enter(job->common.co, job);
}
