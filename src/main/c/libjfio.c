/* SPDX-License-Identifier: Apache-2.0 */
#include <stdlib.h>
#include <liburing.h>
#include <fcntl.h>

// #include <stdio.h>

#include "libjfio.h"

extern struct io_uring* create_ring(int depth, bool enableSQPoll, bool enableIOPoll) {
    unsigned flags = 0;
    if (enableSQPoll) {
        flags |= IORING_SETUP_SQPOLL;
    }
    if (enableIOPoll) {
        flags |= IORING_SETUP_IOPOLL;
    }

    struct io_uring *ring = malloc(sizeof(struct io_uring));
    io_uring_queue_init(depth, ring, flags);
    return ring;
}

extern void submit_and_check_completions(
    struct io_uring* ring,
    const struct submission* submissions,
    int nr_submissions,
    struct submission_and_completion_result *res
) {
    unsigned head;
    struct io_uring_cqe *cqe;
    struct io_uring_sqe *sqe;

    res->nr_submitted = 0;
    res->nr_completed = 0;

    // First submit as much submission as there is room.
    bool has_submitted = false;
    for (int i = 0; i < nr_submissions; i++) {
        sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            break;
        }

        has_submitted = true;
        //fprintf(stdout, "[S %d] id=%ld\n", i, submissions->id);
        //fprintf(stdout, "[S %d] fd=%d\n", i, submissions->fd);
        //fprintf(stdout, "[S %d] address=%p\n", i, submissions->buf_base);
        //fprintf(stdout, "[S %d] offset=%ld\n", i, submissions->offset);
        //fprintf(stdout, "[S %d] length=%d\n", i, submissions->buf_length);
        if (submissions->is_read) {
            io_uring_prep_read(sqe, submissions->fd, submissions->buf_base, submissions->buf_length, submissions->offset);
        } else {
            io_uring_prep_write(sqe, submissions->fd, submissions->buf_base, submissions->buf_length, submissions->offset);
        }
        io_uring_sqe_set_data(sqe, (void*) (uintptr_t) submissions->id);
        submissions++;
        res->nr_submitted++;
    }
    if (has_submitted) {
        io_uring_submit(ring);
    } else if (ring->flags & IORING_SETUP_IOPOLL) {
        io_uring_peek_cqe(ring, &cqe);
    }

    // Now, reap as many completions as there are available.
    unsigned i = 0;
    io_uring_for_each_cqe(ring, head, cqe) {
        //fprintf(stdout, "[C] res = %d\n", cqe->res);
        //fprintf(stdout, "[C %d] completed[%ld] = %ld, \n", i, res->nr_completed, (long) io_uring_cqe_get_data(cqe));
        res->completed_res[res->nr_completed] = cqe->res;
        res->completed_ids[res->nr_completed] = (long) io_uring_cqe_get_data(cqe);
        res->nr_completed++;
        i++;
    }

    io_uring_cq_advance(ring, i);
}

extern void destroy_ring(struct io_uring* ring) {
    io_uring_queue_exit(ring);
    free(ring);
}

extern int open_file(struct io_uring* ring, const char* path, bool readOnly, bool direct) {
    struct io_uring_cqe *cqe;
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        return -1;
    }
    int flags = O_RDWR;
    if (readOnly) {
     flags = O_RDONLY;
    }
    if (direct) {
        flags |= O_DIRECT;
    }
    io_uring_prep_openat(sqe, -1, path, flags, 0);
    io_uring_submit(ring);
    io_uring_wait_cqe(ring, &cqe);

    return cqe->res;
}

extern int close_file(struct io_uring* ring, int fd) {
    struct io_uring_cqe *cqe;
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        return -1;
    }
    io_uring_prep_close(sqe, fd);
    io_uring_submit(ring);
    io_uring_wait_cqe(ring, &cqe);

    return cqe->res;
}

