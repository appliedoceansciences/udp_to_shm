/* campbell, isc license */
#include "shared_memory_ringbuffer.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <stdlib.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <signal.h>

#include <stdatomic.h>

struct shared_memory_ringbuffer_slot {
    /* the non-padded size of the data segment. */
    size_t size;

    unsigned char _Alignas(16) data[];
};

static_assert(!(offsetof(struct shared_memory_ringbuffer_slot, data) % 16), "alignment");

struct shared_memory_ringbuffer {
    /* this is the actual logical capacity of the ring buffer, i.e. the size of the data
     segment minus the maximum slot size. this number MUST be a power of two. when the
     writer sends a new slot, it increments writer_cursor by the size of the just-written
     slot. the effective positions of the writer and reader cursors within the data segment
     are their values modulo this number */
    size_t cursor_wrap;

    /* maximum slot size, which is the requested max packet size plus size of slot prefix */
    size_t max_slot_size;

    /* atomically stored by the writer, and atomically loaded by the readers. the writer
     gets a pointer to the data segment of the slot represented by this value when calling
     acquire(), and atomically stores the incremented value after writing the corresponding
     data. the reader must always assume that the writer could be writing to a value up to
     max_slot_size bytes beyond this value. we don't use size_t because even though we can
     make size_t atomic, we can't do a compile-time assert that size_t is lock-free, as is
     necessary to use it within an shm between processes */
    _Atomic unsigned long writer_cursor;

    /* the writer populates this field, allowing readers to check whether they are
     connecting to an actively-being-written shm segment, or an abandoned one (a condition
     which they should treat the same as if the shm did not yet exist). this value is
     atomically populated with only after the everything else has been initialized. we don't
     use pid_t directly because even though we can make pid_t atomic, we can't do a
     compile-time assert that pid_t is lock-free */
    _Atomic long writer_pid;

    /* the actual ring buffer, which consists of shared_memory_ringbuffer_slots */
    unsigned char _Alignas(16) data[];
};

static_assert(!(offsetof(struct shared_memory_ringbuffer, data) % 16), "alignment");

/* guarantee that writer_cursor and writer_pid are lock-free */
static_assert(2 == ATOMIC_LONG_LOCK_FREE, "long is not lock free");
static_assert(sizeof(long) >= sizeof(pid_t), "cannot store pid_t in long");

struct shared_memory_ringbuffer * shared_memory_ringbuffer_writer_init(const char * name, const size_t ringbuffer_size, const size_t packet_size_max) {
    /* ringbuffer_size must be nonzero and a power of two */
    assert(ringbuffer_size && !(ringbuffer_size & (ringbuffer_size - 1)));

    const size_t max_slot_size = packet_size_max + sizeof(struct shared_memory_ringbuffer_slot);

    /* size of the actual mmap'd region */
    const size_t total_size = offsetof(struct shared_memory_ringbuffer, data) + ringbuffer_size + max_slot_size;

    /* everything must be a multiple of 16 */
    assert(!(packet_size_max % 16));
    assert(!(total_size % 16));

    shm_unlink(name);
    const int fd = shm_open(name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (-1 == fd) {
        fprintf(stderr, "error: %s: shm_open(%s): %s\n", __func__, name, strerror(errno));
        return MAP_FAILED;
    }

    if (-1 == ftruncate(fd, total_size)) {
        fprintf(stderr, "error: %s: ftruncate(): %s\n", __func__, strerror(errno));
        close(fd);
        return MAP_FAILED;
    }

    struct shared_memory_ringbuffer * shm = mmap(NULL, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (MAP_FAILED == shm) {
        fprintf(stderr, "error: %s: mmap(): %s\n", __func__, strerror(errno));
        return MAP_FAILED;
    }

    *shm = (struct shared_memory_ringbuffer) {
        .cursor_wrap = ringbuffer_size,
        .max_slot_size = max_slot_size,
    };

    /* atomic store, must be last thing in this function */
    shm->writer_pid = getpid();

    return shm;
}

void shared_memory_ringbuffer_writer_close(struct shared_memory_ringbuffer * shm) {
    /* indicate to readers that the writer is going away */
    shm->writer_pid = 0;

    const size_t total_size = offsetof(struct shared_memory_ringbuffer, data) + shm->cursor_wrap + shm->max_slot_size;
    munmap(shm, total_size);
}

void * shared_memory_ringbuffer_acquire(struct shared_memory_ringbuffer * shm) {
    struct shared_memory_ringbuffer_slot * const slot = (void *)(shm->data + (shm->writer_cursor % shm->cursor_wrap));
    return slot->data;
}

void shared_memory_ringbuffer_send(struct shared_memory_ringbuffer * shm, const size_t size) {
    size_t writer_cursor = shm->writer_cursor;

    /* populate the prefix fields */
    struct shared_memory_ringbuffer_slot * slot = (void *)(shm->data + (writer_cursor % shm->cursor_wrap));
    slot->size = size;

    /* increment the cursor */
    const size_t size_padded = (sizeof(struct shared_memory_ringbuffer_slot) + slot->size + 15) & ~15;
    assert(size_padded <= shm->max_slot_size);
    writer_cursor += size_padded;

    /* atomically update the globally visible cursor  */
    shm->writer_cursor = writer_cursor;
}

struct shared_memory_ringbuffer_reader {
    struct shared_memory_ringbuffer * shm;
    size_t reader_cursor;
};

int shared_memory_ringbuffer_eof(struct shared_memory_ringbuffer_reader * reader) {
    /* it should be impossible for a reader to call this function on a writer that is not */
    const pid_t writer_pid = reader->shm->writer_pid;
    if (!writer_pid) return 1;

    if (-1 == kill(writer_pid, 0)) {
        if (ESRCH == errno) return 1;
        else if (EPERM != errno) {
            fprintf(stderr, "error: %s: kill(%d): %s\n", __func__, writer_pid, strerror(errno));
            return -1; /* caller can detect this error if they want, or just treat as eof */
        }
    }
    return 0;
}

int shared_memory_ringbuffer_reader_has_kept_up(struct shared_memory_ringbuffer_reader * reader) {
    /* returns 1 if there's no possibility that the most recent read was corrupted by the
     writer having lapped it within the ring buffer, as in the case of a slow reader. this
     should be called by the calling code AFTER it has finished doing something with the
     last-read packet, BEFORE pushing the results of said operations further downstream, in
     order to deterministically handle the slow-reader condition */
    const size_t writer_cursor = reader->shm->writer_cursor;

    /* well-formed even across wraparound */
    const size_t lag = writer_cursor - reader->reader_cursor;

    /* assume the writer could currently be populating a maximum-size packet */
    return lag + reader->shm->max_slot_size <= reader->shm->cursor_wrap;
}

ssize_t shared_memory_ringbuffer_recv(const void ** ret_p, struct shared_memory_ringbuffer_reader * reader) {
    struct shared_memory_ringbuffer * shm = reader->shm;

    /* atomic load */
    const size_t writer_cursor = shm->writer_cursor;

    /* if reader is caught up to writer, return 0 immediately, rather than blocking. the
     reader can sleep or whatever for a context-dependent amount of time before checking again */
    if (writer_cursor == reader->reader_cursor) {
        *ret_p = NULL;
        return 0;
    };

    const struct shared_memory_ringbuffer_slot * const slot = (void *)(shm->data + (reader->reader_cursor % shm->cursor_wrap));
    const size_t slot_size = slot->size;

    /* as soon as we've read the size of the packet, we have to verify that we're not a slow
     reader before we do anything with the size we just read. calling code should react to
     -1 by immediately breaking out of the loop */
    const size_t writer_cursor_after_reading_size = shm->writer_cursor;
    if (writer_cursor_after_reading_size + reader->shm->max_slot_size - reader->reader_cursor - sizeof(struct shared_memory_ringbuffer_slot) > reader->shm->cursor_wrap)
        return -1;

    /* increment the cursor, with possible wraparound */
    const size_t size_padded = (sizeof(struct shared_memory_ringbuffer_slot) + slot_size + 15) & ~15;
    reader->reader_cursor += size_padded;

    *ret_p = slot->data;
    return slot_size;
}

void shared_memory_ringbuffer_reader_close(struct shared_memory_ringbuffer_reader * reader) {
    const size_t total_size = offsetof(struct shared_memory_ringbuffer, data) + reader->shm->cursor_wrap + reader->shm->max_slot_size;
    munmap(reader->shm, total_size);
    free(reader);
}

struct shared_memory_ringbuffer_reader * shared_memory_ringbuffer_reader_init(const char * name) {
    const int fd = shm_open(name, O_RDONLY, 0);
    if (-1 == fd) {
        if (errno == ENOENT) return NULL;
        else {
            fprintf(stderr, "error: %s: shm_open(%s): %s\n", __func__, name, strerror(errno));
            return MAP_FAILED;
        }
    }

    struct stat s;
    if (-1 == fstat(fd, &s)) {
        fprintf(stderr, "error: %s: fstat(%s): %s\n", __func__, name, strerror(errno));
        close(fd);
        return MAP_FAILED;
    }

    struct shared_memory_ringbuffer * shm = mmap(NULL, s.st_size, PROT_READ, MAP_SHARED, fd, 0);
    /* done with this */
    close(fd);

    if (MAP_FAILED == shm) {
        fprintf(stderr, "error: %s: mmap(%s): %s\n", __func__, name, strerror(errno));
        return MAP_FAILED;
    }

    /* atomic load, must be the first thing we do before reading any other shm variables */
    const pid_t writer_pid = shm->writer_pid;
    if (!writer_pid) {
        /* writer is not yet finished initializing, treat as if writer does not exist yet */
        munmap(shm, s.st_size);
        return NULL;
    }

    if (-1 == kill(writer_pid, 0) && errno != EPERM) {
        munmap(shm, s.st_size);
        if (errno != ESRCH) {
            fprintf(stderr, "error: %s: kill(%ld): %s\n", __func__, shm->writer_pid, strerror(errno));
            return MAP_FAILED;
        }
        return NULL;
    }

    struct shared_memory_ringbuffer_reader * reader = malloc(sizeof(struct shared_memory_ringbuffer_reader));
    assert(reader);
    *reader = (struct shared_memory_ringbuffer_reader) {
        .shm = shm,
        .reader_cursor = shm->writer_cursor
    };

    return reader;
}
