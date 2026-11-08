#!/usr/bin/env python3
import sys
import struct

from shared_memory_ringbuffer_reader import shared_memory_ringbuffer_generator

def main():
    shm_name = '/acoustic_packets' if len(sys.argv) < 2 else sys.argv[1]

    for packet_with_logging_header in shared_memory_ringbuffer_generator(shm_name):
        packet_size, timestamp_lsbs, timestamp_msbs = struct.unpack('<HHI', packet_with_logging_header[0:8])
        packet_size_sanity_check = len(packet_with_logging_header) - 8

        if packet_size != packet_size_sanity_check:
            print('warning: packet size discrepancy: %u != %u', file=sys.stderr)
            continue

        packet_bytes = packet_with_logging_header[8:(packet_size + 8)]

        try:
            tmp = str(packet_bytes, 'utf-8')
            if tmp.strip().isprintable(): print(tmp)
        except: continue

main()
