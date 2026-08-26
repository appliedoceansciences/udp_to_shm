/*
 Copyright 2022- Applied Ocean Sciences

 Permission to use, copy, modify, and/or distribute this software for any purpose with or
 without fee is hereby granted, provided that the above copyright notice and this
 permission notice appear in all copies.

 THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO
 THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT
 SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR
 ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE
 USE OR PERFORMANCE OF THIS SOFTWARE.

 Zero-copy shared memory fanout for datagrams arriving via UDP

 For each received datagram, an eight-byte logging header is prepended, consisting of a
 little- endian unsigned 16-bit integer representing the size of the packet (not including
 the logging header), and a 48-bit little-endian unsigned integer representing the unix
 epoch time in increments of sixteen microseconds at which the packet was received.

 Care should be taken to ensure that the system clock is synced to a GPS or precision RTC
 time reference prior to starting this code, and ideally continually therafter.
 */

/* library functions */
#include "shared_memory_ringbuffer.h"

/* c standard includes */
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <stdint.h>
#include <assert.h>
#include <signal.h>
#include <ctype.h>

/* posix includes */
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>

/* useful macros */
#define WARNING_ANSI "\x1B[35;1mwarning:\x1B[0m"
#define ERROR_ANSI "\x1B[31;1merror:\x1B[0m"
#define NOPE(...) do { fprintf(stderr, ERROR_ANSI " " __VA_ARGS__); exit(EXIT_FAILURE); } while(0)

static unsigned long long current_time_in_unix_microseconds(void) {
    struct timespec timespec;
    clock_gettime(CLOCK_REALTIME, &timespec);
    return timespec.tv_sec * 1000000ULL + timespec.tv_nsec / 1000UL;
}

volatile sig_atomic_t got_sigterm_or_sigint = 0;

static void sigint_handler(int sig) {
    (void)sig;
    got_sigterm_or_sigint = 1;
}

static int text_packet(void * packet_buffer, const size_t packet_size) {
    unsigned char * restrict const byte = packet_buffer;

    size_t printable_characters = 0;
    for ( ; printable_characters < packet_size; printable_characters++) {
        if (byte[printable_characters] == '\r' ||
            byte[printable_characters] == '\n') break;
        if (!isprint(byte[printable_characters])) return 0;
    }

    if (printable_characters)
        fprintf(stderr, "%s: \"%.*s\"\n", __func__, (int)printable_characters, byte);
    return 1;
}

int main(const int argc, char ** const argv) {
    /* do some silly stuff to get a progname regardless of runtime environment */
    const char * s, * progname = argc ? ((s = strrchr(argv[0], '/')) ? s + 1 : argv[0]) : __func__;

#ifdef GIT_VERSION
    fprintf(stderr, "%s: built from commit %s\n", progname, GIT_VERSION);
#endif

    /* install a signal handler so that we can stop cleanly on sigint or sigterm */
    if (-1 == sigaction(SIGINT, &(struct sigaction) { .sa_handler = sigint_handler }, NULL) ||
        -1 == sigaction(SIGTERM, &(struct sigaction) { .sa_handler = sigint_handler }, NULL))
        NOPE("%s: sigaction(): %s\n", progname, strerror(errno));

    const unsigned short udp_input_port = 24597;
    const unsigned short udp_heartbeat_dest_port = 24598;
    const unsigned heartbeat_interval_us = 500000;
    const char * heartbeat_ip = NULL;
    const char * shm_name = "/acoustic_packets";

    /* handle --[key]=[value] or space-separated [key] [value] argument pairs */
    for (int iarg = 1; iarg < argc; iarg++) {
        char * key = argv[iarg] + (!strncmp(argv[iarg], "--", 2) ? 2 : 0);
        const size_t keylen = strcspn(key, "=");
        const char * val = '=' == key[keylen] ? key + keylen + 1 : argv[iarg + 1] ? argv[++iarg] : "";
        key[keylen] = '\0';

        if (!strcmp(key, "heartbeat")) heartbeat_ip = val;
        else if (!strcmp(key, "shm")) shm_name = val;
        else NOPE("%s: %s %s: argument unrecognized\n", argv[0], key, val);
    }

    struct sockaddr_in peer = {
        .sin_family = AF_INET,
        .sin_port = htons(udp_heartbeat_dest_port)
    };
    if (heartbeat_ip) {
        if (inet_pton(AF_INET, heartbeat_ip, &peer.sin_addr) != 1) abort();
        fprintf(stderr, "%s: sending heartbeats to %s:%u every %u ms\r\n", progname, heartbeat_ip, udp_heartbeat_dest_port, (heartbeat_interval_us + 500) / 1000);
    }

    /* only slightly cargo cult scheduling stuff */
    if (-1 == setpriority(0, PRIO_PROCESS, -20))
        fprintf(stderr, WARNING_ANSI " %s: failed to set priority, adjust RLIMIT_NICE\n", progname);
    mlockall(MCL_CURRENT | MCL_FUTURE);

    /* logging header plus maximum size of packet, must be a multiple of 16 */
    struct {
        uint64_t logging_header;
        unsigned char packet[2040];
    } * buf = NULL;

    static_assert(!(sizeof(*buf) % 16), "max shared memory slot size must be a multiple of 16");

    /* establish a shared-memory segment into which we will place the incoming
     packets, allowing them to be shared with zero or more listening downstream
     processes in a zero-copy scheme, with no possibility of a slow reader
     blocking the writer or other readers */
    struct shared_memory_ringbuffer * shm = shared_memory_ringbuffer_writer_init(shm_name, 4194304, sizeof(*buf));
    if (MAP_FAILED == shm || !shm) exit(EXIT_FAILURE);

    /* sleep a bit to give simultaneously-started readers a chance to connect for determinism */
    usleep(200000);

    int fd_udp = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (-1 == fd_udp) NOPE("%s: cannot socket(): %s\n", progname, strerror(errno));

    if (-1 == bind(fd_udp, (struct sockaddr *)&(struct sockaddr_in) {
        .sin_family = AF_INET,
        .sin_port = htons(udp_input_port),
        .sin_addr.s_addr = htonl(INADDR_ANY)
    }, sizeof(struct sockaddr_in)))
        NOPE("%s: cannot bind(%d): %s\n", progname, udp_input_port, strerror(errno));

    if (heartbeat_ip && -1 == sendto(fd_udp, "heartbeat\r\n", 11, 0, (void *)&peer, sizeof(peer)))
        fprintf(stderr, "warning: %s: failed to send to %u: %s\n", progname, ntohs(peer.sin_port), strerror(errno));

    unsigned long long packet_time_previous = 0;

    /* get the next slot in the ring buffer */
    buf = shared_memory_ringbuffer_acquire(shm);

    unsigned char beginning_of_last_packet[16];
    size_t sizeof_heartbeat = 0;
    unsigned long long heartbeat_time_previous = 0;

    /* loop until we get the first packet, possibly sending heartbeats */
    ssize_t recv_ret = 0;
    while (recv_ret <= 0) {
        if (heartbeat_ip && -1 == sendto(fd_udp, "heartbeat\r\n", 11, 0, (void *)&peer, sizeof(peer)))
            fprintf(stderr, "warning: %s: failed to send to %u: %s\n", progname, ntohs(peer.sin_port), strerror(errno));

        recv_ret = recv(fd_udp, buf->packet, sizeof(buf->packet), 0);
        if (recv_ret < 0) {
            /* if we timed out, just keep sending another heartbeat and repeating */
            if (EAGAIN == errno || EWOULDBLOCK == errno) continue;
            fprintf(stderr, ERROR_ANSI " %s: could not recv(): %s\n", progname, strerror(errno));
            return -1;
        }
    }

    /* loop over incoming udp packets */
    do {
        if (got_sigterm_or_sigint) break;

        if (recv_ret < 0) {
            /* if we timed out, assume no more data is coming */
            if (EAGAIN == errno || EWOULDBLOCK == errno) break;
            fprintf(stderr, ERROR_ANSI " %s: could not recv(): %s\n", progname, strerror(errno));
            return -1;
        }

        const size_t packet_size = recv_ret;

        const unsigned long long packet_time_microseconds = current_time_in_unix_microseconds();
        if (packet_time_previous > packet_time_microseconds)
            fprintf(stderr, WARNING_ANSI " %s: time has jumped backwards by %lld us, new time is %llu\n",
                    progname, packet_time_previous - packet_time_microseconds, packet_time_microseconds);
        packet_time_previous = packet_time_microseconds;

        buf->logging_header = ((packet_time_microseconds / 16) << 16) | packet_size;

        /* round packet size up to the next multiple of 8, and write up to 7 bytes of padding, s.t.
         the next packet will be eight-byte-aligned within the output */
        const size_t packet_size_padded = (packet_size + 7) & ~7;

        /* zero out any padding we're going to write */
        if (packet_size_padded != packet_size)
            memset(buf->packet + packet_size, 0, packet_size_padded - packet_size);

        /* release to readers */
        shared_memory_ringbuffer_send(shm, sizeof(buf->logging_header) + packet_size);

        text_packet(buf->packet, packet_size);

        sizeof_heartbeat = packet_size < 16 ? packet_size : 16;
        memcpy(beginning_of_last_packet, buf->packet, sizeof_heartbeat);

        if (packet_time_microseconds - heartbeat_time_previous >= heartbeat_interval_us) {
            if (heartbeat_ip && -1 == sendto(fd_udp, beginning_of_last_packet, sizeof_heartbeat, 0, (void *)&peer, sizeof(peer)))
                fprintf(stderr, "warning: %s: failed to send to %u: %s\n", progname, ntohs(peer.sin_port), strerror(errno));
            heartbeat_time_previous = packet_time_microseconds;
            sizeof_heartbeat = 0;
        }

        /* get the next slot in the ring buffer */
        buf = shared_memory_ringbuffer_acquire(shm);
    } while ((recv_ret = recv(fd_udp, buf->packet, sizeof(buf->packet), 0)) > 0);

    if (sizeof_heartbeat) {
        if (heartbeat_ip && -1 == sendto(fd_udp, beginning_of_last_packet, sizeof_heartbeat, 0, (void *)&peer, sizeof(peer)))
            fprintf(stderr, "warning: %s: failed to send to %u: %s\n", progname, ntohs(peer.sin_port), strerror(errno));
    }

    fprintf(stderr, "%s: exiting\n", progname);

    close(fd_udp);

    return 0;
}
