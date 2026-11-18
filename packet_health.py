#!/usr/bin/env python3
# Usage: ./shm_to_pipe | ./packet_health.py
# Usage: ssh remotehost shm_to_pipe | ./packet_health.py
import sys
import numpy as np

# This provides the generator function which knows how to extract sensor-agnostic frames of
# acoustic sample data from whatever possibly sensor-specific format they are coming from
from parse_acoustic_packets import yield_acoustic_packets, yield_packet_bytes_from_log_stream, datestr_from_unix_microseconds

def main():
    # start a child generator function which ingests input from stdin and yields acoustic packets
    child = yield_acoustic_packets(yield_packet_bytes_from_log_stream, sys.stdin.buffer, None)

    # wait for first packet
    packet = next(child, None)
    if not packet: return

    # do any extra init that requires the first packet to have been received
    print('%u channels, %g samples per second' % (packet.samples.shape[1], packet.fs))

    while packet:
        # put code here to do whatever you want with each packet, in this case just print the min and max
        print('          \rtime: %s: min: %d, max: %d' % (
            datestr_from_unix_microseconds(packet.timestamp_microseconds),
            np.min(packet.samples), np.max(packet.samples)
        ), file=sys.stderr, end='')

        packet = next(child, None)

    print('', file=sys.stderr)

main()
