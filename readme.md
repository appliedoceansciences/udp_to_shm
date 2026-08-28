# `udp_to_shm`

This repository implements the Linux end of the interface between a DAQ microcontroller and soft-realtime logging and processing environment on a Linux SBC (or Mac/Linux laptop) via UDP packets. The expected mechanism is UDP over USB CDC Ethernet, or physical Ethernet.

This method and repository supersedes [cobs_to_shm](https://github.com/appliedoceansciences/cobs_to_shm), which moved the same framed data over the same physical link using UDP over USB serial on earlier DAQ implementations. Some design decisions are aimed at providing backward compatibility for downstream applications which expect a `cobs_to_shm` service and shared-memory segment name.

## Functional description

Upon startup, the `udp_to_shm` binary initializes a ring buffer in a shared-memory segment and opens a UDP port. Every received packet is prepended with an 8-byte logging header (see below) and placed into the shared-memory ring buffer.

### Heartbeats

Assuming a heartbeat IP address argument is given, the code will initially send UDP packets to the expected sender port at the given IP containing the string "heartbeat" until data packets are recieved. Subsequent heartbeat packets, containing the first 16 bytes of the most recently received data packet will continue to be sent every few hundred milliseconds, as long as data packets continue to be received. After a timeout with no received packets, the code will terminate, sending one final heartbeat packet consisting of the first 16 bytes of the final received data packet.

### Shared-memory segment name

The code itself defaults to using `/acoustic_packets` as the name of the shared-memory segment if none is specified. As of this writing, the included example .service files specify `--shm /cobs_to_shm` in order to preserve backward compatibility with downstream code expecting the latter name. The format of the packets within the shared-memory ring buffer is the same regardless of which of `udp_to_shm` or `cobs_to_shm` is writing to it.

## Building

Invoke `make` in this repository, with no argument, to compile the code. Optionally, invoke `make install` as root to copy the resulting binary and example `.service` files to `/usr/local/bin/` and `/etc/systemd/system/` respectively, if applicable.

## Usage

Invoke with an argument specifying the remote IP to which heartbeat messages should be sent:

    ./udp_to_shm --heartbeat 192.168.7.1

Start an additional reader for logging, and pipe the output into logic which will move the resulting files to some final path:

    ./shm_logger | xargs -I file mv file /final/path/

Example `.service` files are included which invoke the `udp_to_shm` and `shm_logger` binaries with appropriate arguments. Note that these assume a `daq` user with a sub-1000 uid (so that systemd does not delete the shm segment) whose home directory contains the destination directory for the resulting logged data. Adjust this logic according to your needs, or create a `daq` user with a sub-1000 uid and associated home directory using `useradd -rm daq`.

An example udev rule is included which sets a static IP address after a short delay when the device is enumerated. This can be used to, for example, get data flowing before NetworkManager or another DHCP client is fully started.

## Logged data

The resulting `.bin` files contain a stream of acoustic and possibly nonacoustic packets, each prefixed with an eight byte header containing a packet size and timetamp. Up to seven bytes of padding is added after each packet, if necessary, to ensure that the beginning of the next packet is aligned to eight bytes. The beginnings of the `.bin` files carry no significance and are simply aligned with wall clock time on a best-effort basis - that is, multiple consecutive `.bin` files concatenated together are also a valid `.bin` file, with no gaps. Similarly, multiple `.bin.gz` files can be concatenated together and piped through `gunzip` as if they had always been a single file.

The acoustic packets consist of a header prepended to a block of samples. The samples are signed 16-bit little endian integers, such that the various headers can be peeled off each acoustic packet and the data segments concatenated together, and the result can be interpreted as a continuous stream of PCM audio samples. The included `parse_acoustic_packets.py` script can perform this operation, as follows:

    cat /path/to/*.bin | ./parse_acoustic_packets.py > combined_raw_pcm_audio.raw

The resulting raw PCM audio can be processed as-is, or a `.wav` header can be prepended to it if necessary:

    ./prepend_wav_header.py < combined_raw_pcm_audio.raw > /tmp/combined_audio.wav

Note that since `.wav` files must include the file length in the header, it is not possible to prepend a `.wav` header to a stream of data - a temporary file of not more than 4 gigabytes must first be created. If streaming operation is necessary, omit the `.wav` header and operate on just the raw PCM data.

## Components

### Standalone applications

- `udp_to_shm`: The main application, written in C, which sends heartbeats, receives UDP packets, and fans out the resulting stream of packets to the shared memory ring buffer and optional logging

- `shm_logger`: Standalone logger that consumes packets from the ring buffer and writes them to disk, in a format suitable for replay. This also serves as an example ring buffer reader application in C.

- `shm_to_pipe`: Minimum viable C standalone process that consumes packets from the ring buffer and writes them to stdout in the same logging format emitted to disk by `shm_logger`. Functionally equivalent to `shared_memory_ringbuffer_reader.py` when the latter is invoked as a standalone process, but with less overhead. Typically used as the upstream end of soft-realtime DSP pipelines which consist of multiple processes piped together (possibly with an ssh pipe in between processes).

### Modules used by the above

- `shared_memory_ringbuffer_reader.py` and `shared_memory_ringbuffer.c`: Python and C modules with functions to read from the shared memory ring buffer and return packets one at a time to calling code. The Python module can also be run as a standalone process, and will yield the stream of packets to `stdout` in the same logging format emitted by `shm_logger`, although see `shm_to_pipe` above for a lower-overhead version of the same functionality.

- `parse_acoustic_packets.py`: Python module which ingests the acoustic packets and yields packets worth of samples at a time to calling code, suitable for developing soft-realtime DSP applications. Can be run as a standalone process, which will ingest the logging format emitted by `shm_logger` and yield raw PCM on `stdout`, suitable for piping into `ffmpeg` or any other software which expects PCM.
