/* SPDX-License-Identifier: Apache-2.0 */
/*
 * Simple wrapper library around liburing for use by jfio.
 *
 * The goal of this wrapper is essentially to provide a relatively simple interface to call from Java, without needed
 * to (re)declare a ton of constants, and also to minimize the number of calls into native code we need to do.
 *
 * This is not meant to be used for any other purpose (and some of the functions work only if used under specific
 * assumptions).
 */
#ifndef LIBJFIO_H__
#define LIBJFIO_H__

#include <sys/uio.h>
#include <liburing.h>

// A submission for the `submit_and_check_completions` function. Only supports reads at present.
struct submission {
    int id;         // Id of the submission (how we'll identify when this submission completes).
    int fd;         // File descriptor on which the read operates
    int buf_length; // Length of the buffer to read into.
    void* buf_base; // Base address of the buffer to read to.
    long offset;    // Offset in the file at which to read.
};

// Stores the result of a `submit_and_check_completions` call.
struct submission_and_completion_result {
    int nr_submitted;    // number of submissions actually submitted to the ring.
    int nr_completed;    // number of completion found (and reaped).
    // Note that the `submit_and_check_completions function _assumes_ that the follow arrays are large enough to store
    // entries for all the completions found (tl;dr, those array should have size `depth * 2`).
    int* completed_res;  // results of the completions found.
    int* completed_ids; // ids of the completions found.
};

/*
 * Submits up to `nr_submissions` submissions from `submissions` to the provided ring, and then reap as much
 * completions as possible (_without_ blocking). There is no guarantee on how many submissions will actually be
 * submitted; this depend on how much room the submission queue has. Note that `nr_submissions` can be 0 if we only
 * want to reap completions.
 */
extern void submit_and_check_completions(
    struct io_uring* ring,
    const struct submission* submissions,
    int nr_submissions,
    struct submission_and_completion_result *result
);

/* Creates a new ring with the provided `depth`. */
extern struct io_uring* create_ring(int depth, bool enableSQPoll, bool enableIOPoll);

/* Destroy the provided ring. */
extern void destroy_ring(struct io_uring* ring);

/*
 * Submit an "openat" request to the provided ring for the provided file, and wait on it's completion (returning the
 * resulting fd (or error). The `direct` flag allows the file to be opened with O_DIRECT (and this is the main
 * reason for this to exists: we can open a file from Java, but not with O_DIRECT). This currently only ever
 * open the file in read-only mode.
 *
 * This function assumes that the ring is _empty_ when this is called.
 */
extern int open_file(struct io_uring* ring, const char* path, bool direct);

/*
 * Closes a file opened with `open_file` (this is also a synchronous operation which waits on the completion).

 * This function assumes that the ring is _empty_ when this is called.
 */
extern int close_file(struct io_uring* ring, int fd);

#endif
