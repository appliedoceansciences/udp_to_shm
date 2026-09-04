#!/usr/bin/env python3
import sys
import struct
from datetime import datetime, timezone

from shared_memory_ringbuffer_reader import shared_memory_ringbuffer_generator

def datestr_from_unix_microseconds(microseconds):
    return '%s.%06uZ' % (datetime.fromtimestamp(microseconds // 1000000, timezone.utc).strftime('%Y%m%dT%H%M%S'), microseconds % 1000000)

def validate_acoustic_packet(packet_bytes):
    # if packet size is too small to be an acoustic packet, skip it
    if len(packet_bytes) < 16: return None

    # parse the packet header
    magic, channels, seqnum, sample_rate, flags, timestamp_lsbs, timestamp_msbs = struct.unpack('<BBHfHHI', packet_bytes[0:16])

    # validate packet header, part one
    if magic != 0x45: return None

    # interpret lowest three bits of flags field as the data type
    dtype = 'h' if (flags & 0x7 == 0) else 'i' if (flags & 0x7 == 1) else 'f' if (flags & 0x7 == 3) else 'i1' if (flags & 0x7 == 0b100) else 'int24'

    sizeof_sample = 2 if (dtype == 'h') else 4 if (dtype == 'i') else 3 if (dtype == 'int24') else 1 if dtype == 'i1' else 4
    samples_per_channel_per_packet = (len(packet_bytes) - 16) // (channels * sizeof_sample)

    # validate packet header, part two
    if samples_per_channel_per_packet * sizeof_sample * channels + 16 != len(packet_bytes): return None

    # reassemble timestamp in unix seconds
    timestamp_microseconds = ((timestamp_msbs << 16) | timestamp_lsbs) * 16

    return channels, seqnum, sample_rate, timestamp_microseconds, samples_per_channel_per_packet

def main():
    shm_name = '/acoustic_packets' if len(sys.argv) < 2 else sys.argv[1]

    seqnum_prev = None
    initial_timestamp = None
    timestamp_prev = None
    samples_yielded = 0

    for packet_with_logging_header in shared_memory_ringbuffer_generator(shm_name):
        packet_size, timestamp_lsbs, timestamp_msbs = struct.unpack('<HHI', packet_with_logging_header[0:8])
        logged_timestamp_microseconds = ((timestamp_msbs << 16) | timestamp_lsbs) * 16
        packet_size_sanity_check = len(packet_with_logging_header) - 8

        if packet_size != packet_size_sanity_check:
            print('warning: packet size discrepancy: %u != %u', file=sys.stderr)
            continue

        packet_bytes = packet_with_logging_header[8:(packet_size + 8)]

        packet_info = validate_acoustic_packet(packet_bytes)
        if packet_info:
            channels, seqnum, sample_rate, timestamp_microseconds, samples_per_channel_per_packet = packet_info

            # emit some diagnostic text on the first packet
            if seqnum_prev is None:
                initial_timestamp = timestamp_microseconds - round(samples_per_channel_per_packet * 1e6 / sample_rate)
                print('first packet timestamp %s (%u.%06u), arrived at %s' %
                    (datestr_from_unix_microseconds(timestamp_microseconds),
                    timestamp_microseconds // 1000000, timestamp_microseconds % 1000000,
                     datestr_from_unix_microseconds(logged_timestamp_microseconds)), file=sys.stderr)
                print('%u channels, %g sps, %u samples per channel per packet' %
                      (channels, sample_rate, samples_per_channel_per_packet), file=sys.stderr)
            else:
                packets_missing = (seqnum - seqnum_prev - 1) % 65536
                if packets_missing != 0:
                    print('warning: at %s / %s: expected seqnum %u, got %u, missing %u packets' % (datestr_from_unix_microseconds(timestamp_microseconds), datestr_from_unix_microseconds(logged_timestamp_microseconds), (seqnum_prev + 1) % 65536, seqnum, packets_missing), file=sys.stderr)
            seqnum_prev = seqnum
            timestamp_prev = timestamp_microseconds
            samples_yielded += samples_per_channel_per_packet
        else:
            try:
                tmp = str(packet_bytes, 'utf-8')
                if tmp.strip().isprintable():
                    print(tmp, file=sys.stderr)
            except: continue

    print('final packet ends at %s' % (datestr_from_unix_microseconds(timestamp_prev)), file=sys.stderr)
    print('%g seconds according to expected sample rate, %g seconds according to packet timestamps' %
          (samples_yielded / sample_rate, (timestamp_prev - initial_timestamp) / 1e6), file=sys.stderr)

main()
