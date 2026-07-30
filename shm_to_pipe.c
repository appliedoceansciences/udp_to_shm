/* this works identically to shared_memory_ringbuffer_reader.py when the latter
 is invoked as a standalone process, but is in C instead of python */
#include "shared_memory_ringbuffer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <stdint.h>

/* useful macros */
#define WARNING_ANSI "\x1B[35;1mwarning:\x1B[0m"
#define ERROR_ANSI "\x1B[31;1merror:\x1B[0m"
#define NOPE(...) do { fprintf(stderr, ERROR_ANSI " " __VA_ARGS__); exit(EXIT_FAILURE); } while(0)

static volatile sig_atomic_t got_sigterm_or_sigint = 0;

static void sigint_handler(int sig) {
    (void)sig;
    got_sigterm_or_sigint = 1;
}

int main(int argc, char ** const argv) {
    /* do some silly stuff to get a progname regardless of runtime environment */
    const char * s, * progname = argc ? ((s = strrchr(argv[0], '/')) ? s + 1 : argv[0]) : __func__;

    const char * shm_name = argc > 1 ? argv[1] : "/cobs_to_shm";

    /* ensure that stdout will be unbuffered */
    setvbuf(stdout, NULL, _IONBF, 0);

    /* install a signal handler so that we can stop cleanly on sigint or sigterm */
    if (-1 == sigaction(SIGINT, &(struct sigaction) { .sa_handler = sigint_handler }, NULL) ||
        -1 == sigaction(SIGTERM, &(struct sigaction) { .sa_handler = sigint_handler }, NULL))
        NOPE("%s: sigaction(): %s\n", progname, strerror(errno));

    struct shared_memory_ringbuffer_reader * shm = NULL;
    char printed_not_ready = 0;

    /* loop until the writer exists */
    while (!(shm = shared_memory_ringbuffer_reader_init(shm_name))) {
        if (!printed_not_ready) {
            fprintf(stderr, "%s: waiting for \"%s\"\n", progname, shm_name);
            printed_not_ready = 1;
        }
        usleep(50000);
        if (got_sigterm_or_sigint) return 0;
    }

    fprintf(stderr, "%s: connected\n", progname);

    unsigned long usec_per_packet_num = 0, usec_per_packet_den = 0;
    unsigned long delay = 20000;

    while (1) {
        /* low priority TODO: absorb this boilerplate into a utility function */
        size_t packet_size_with_logging_header = 0;
        const void * packet_buffer_with_logging_header = NULL;
        while (1) {
            if (got_sigterm_or_sigint) break;
            const ssize_t status = shared_memory_ringbuffer_recv(&packet_buffer_with_logging_header, shm);
            if (status > 0) {
                packet_size_with_logging_header = status;
                break;
            }
            else if (-1 == status) {
                fprintf(stderr, "%s %s: reader failed to keep up with writer\n", ERROR_ANSI, progname);
                break;
            }

            if (usec_per_packet_num > 0 && shared_memory_ringbuffer_eof(shm)) {
                /* only check for eof if we've already slept and there are still no packets */
                fprintf(stderr, "%s: writer has exited\n", progname);
                break;
            }

            /* sleep for an amount of time that attempts to adapt to the actual data rate.
             if this sleep-and-poll logic bothers you a bit, that's healthy, but definitely
             don't look at how other publish-subscribe mechanisms work under the hood */
            usleep(delay);
            usec_per_packet_num += delay;
        }

        /* if we broke out of the above loop without a packet, we are eof or error */
        if (!packet_buffer_with_logging_header) break;

        if (usec_per_packet_num > 0 && usec_per_packet_den > 0) {
            delay = (3UL * delay + (usec_per_packet_num + usec_per_packet_den / 2UL) / usec_per_packet_den + 2UL) / 4UL;
            delay = delay > 1000000UL ? 1000000UL : delay < 20000UL ? 20000UL : delay;
            usec_per_packet_den = 0;
        }

        usec_per_packet_num = 0;
        usec_per_packet_den++;

        /* got a packet */
        if (packet_size_with_logging_header < sizeof(uint64_t)) {
            fprintf(stderr, "%s %s: skipping packet too small for logging header\n", WARNING_ANSI, progname);
            continue;
        }

        uint64_t logging_header;
        memcpy(&logging_header, packet_buffer_with_logging_header, sizeof(uint64_t));

        const size_t packet_size = logging_header & 65535U;

        if (packet_size_with_logging_header - sizeof(uint64_t) != packet_size) {
            fprintf(stderr, "%s %s: inconsistent packet size\n", WARNING_ANSI, progname);
            continue;
        }

        /* round packet size up to the next multiple of 8, and write up to 7 bytes of
         padding, s.t. the next packet will be eight-byte-aligned within the output */
        const size_t packet_size_padded = (packet_size + 7) & ~7;

        /* write the packet with logging header and padding to stdout */
        if (!fwrite(packet_buffer_with_logging_header, sizeof(uint64_t) + packet_size_padded, 1, stdout))
            NOPE("%s: fwrite(): %s\n", progname, strerror(errno));

        /* ideally, call this AFTER doing whatever that reads the packet contents, BEFORE
         pushing any resulting data further downstream */
        if (!shared_memory_ringbuffer_reader_has_kept_up(shm)) {
            fprintf(stderr, "%s: reader failed to keep up with writer\n", progname);
            break;
        }
    }

    shared_memory_ringbuffer_reader_close(shm);
}
