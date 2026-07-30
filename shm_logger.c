#include "shared_memory_ringbuffer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <stdint.h>

/* useful macros */
#define WARNING_ANSI "\x1B[35;1mwarning:\x1B[0m"
#define ERROR_ANSI "\x1B[31;1merror:\x1B[0m"
#define NOPE(...) do { fprintf(stderr, ERROR_ANSI " " __VA_ARGS__); exit(EXIT_FAILURE); } while(0)
#define alloc_sprintf(...) ({ char * _tmp; if (asprintf(&_tmp, __VA_ARGS__) <= 0) abort(); _tmp ; })

volatile sig_atomic_t got_sigterm_or_sigint = 0;

static void sigint_handler(int sig) {
    (void)sig;
    got_sigterm_or_sigint = 1;
}

int main(int argc, char ** const argv) {
    /* do some silly stuff to get a progname regardless of runtime environment */
    const char * s, * progname = argc ? ((s = strrchr(argv[0], '/')) ? s + 1 : argv[0]) : __func__;

#ifdef GIT_VERSION
    fprintf(stderr, "%s: built from commit %s\n", progname, GIT_VERSION);
#endif

    const char * shm_name = argc > 1 ? argv[1] : "/cobs_to_shm";
    const char * logging_path = argc > 2 ? argv[2] : "/dev/shm";

    /* ensure that stdout will not be full-buffered */
    setvbuf(stdout, NULL, _IOLBF, 0);

    /* install a signal handler so that we can stop cleanly on sigint or sigterm */
    if (-1 == sigaction(SIGINT, &(struct sigaction) { .sa_handler = sigint_handler }, NULL) ||
        -1 == sigaction(SIGTERM, &(struct sigaction) { .sa_handler = sigint_handler }, NULL))
        NOPE("%s: sigaction(): %s\n", progname, strerror(errno));

    struct shared_memory_ringbuffer_reader * shm = NULL;
    char printed_not_ready = 0;

    /* loop until the writer exists */
    while (!(shm = shared_memory_ringbuffer_reader_init(shm_name))) {
        if (!printed_not_ready) {
            fprintf(stderr, "%s: waiting for \"%s\"\n", __func__, shm_name);
            printed_not_ready = 1;
        }
        usleep(50000);
        if (got_sigterm_or_sigint) return 0;
    }

    fprintf(stderr, "%s: connected\n", progname);

    unsigned long usec_per_packet_num = 0, usec_per_packet_den = 0;
    unsigned long delay = 20000;

    unsigned long long time_microseconds_file_start = 0;
    FILE * fh = NULL;
    char * path = NULL;

    while (1) {
        unsigned long long packet_time_microseconds = 0;

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
                fprintf(stderr, "%s %s: reader failed to keep up with writer\n", ERROR_ANSI, __func__);
                break;
            }

            if (usec_per_packet_num > 0 && shared_memory_ringbuffer_eof(shm)) {
                /* only check for eof if we've already slept and there are still no packets */
                fprintf(stderr, "%s: writer has exited\n", __func__);
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
            fprintf(stderr, "%s %s: skipping packet too small for logging header\n", WARNING_ANSI, __func__);
            continue;
        }

        uint64_t logging_header;
        memcpy(&logging_header, packet_buffer_with_logging_header, sizeof(uint64_t));

        packet_time_microseconds = (logging_header >> 16U) * 16U;
        const size_t packet_size = logging_header & 65535U;

        if (packet_size_with_logging_header - sizeof(uint64_t) != packet_size) {
            fprintf(stderr, "%s %s: inconsistent packet size\n", WARNING_ANSI, __func__);
            continue;
        }

        const unsigned long long packet_time_microseconds_rounded_down_to_10s = packet_time_microseconds - (packet_time_microseconds % 10000000ULL);

        /* if rounding down gives a time later than the file start time, we need to close
         the old file and then create a new one in the next step */
        if (fh && packet_time_microseconds_rounded_down_to_10s > time_microseconds_file_start) {
            fclose(fh);
            printf("%s\n", path);
            free(path);
            fh = NULL;
        }

        /* if we just closed the most recent file or haven't opened one yet, open a new one */
        if (!fh && logging_path) {
            /* construct timestamp in ISO 8601 format, no separators, rounded down to seconds */
            struct tm unixtime_struct;
            gmtime_r(&(time_t) { packet_time_microseconds / 1000000ULL }, &unixtime_struct);
            char timestamp[17];
            strftime(timestamp, 17, "%Y%m%dT%H%M%SZ", &unixtime_struct);

            path = alloc_sprintf("%s/%s.bin", logging_path, timestamp);
            fh = fopen(path, "w");
            if (!fh) NOPE("%s: fopen(%s): %s\n", progname, path, strerror(errno));
            time_microseconds_file_start = packet_time_microseconds;
            /* would be nice to write to stderr here, but even logged writes to stderr can block */
        }

        /* round packet size up to the next multiple of 8, and write up to 7 bytes of
         padding, s.t. the next packet will be eight-byte-aligned within the output */
        const size_t packet_size_padded = (packet_size + 7) & ~7;

        /* write the packet to the current output file */
        if (fh && !fwrite(packet_buffer_with_logging_header, sizeof(uint64_t) + packet_size_padded, 1, fh))
            NOPE("%s: fwrite(): %s\n", progname, strerror(errno));

        /* ideally, call this AFTER doing whatever that reads the packet contents, BEFORE
         pushing any resulting data further downstream */
        if (!shared_memory_ringbuffer_reader_has_kept_up(shm)) {
            fprintf(stderr, "%s: reader failed to keep up with writer\n", progname);
            break;
        }
    }

    if (fh) {
        fclose(fh);
        printf("%s\n", path);
        free(path);
    }

    shared_memory_ringbuffer_reader_close(shm);
}
