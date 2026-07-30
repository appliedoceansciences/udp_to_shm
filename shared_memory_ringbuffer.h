/* campbell, isc license */
#include <unistd.h>
#include <stddef.h>

/* calling code needs definition of MAP_FAILED for error handling */
#include <sys/mman.h>

/* writer functions: */

/* writer calls this to create an shm segment. if an error occurs, this function prints to
 stderr and returns MAP_FAILED */
struct shared_memory_ringbuffer * shared_memory_ringbuffer_writer_init(const char * name, const size_t total_size, const size_t packet_size_max);

/* writer calls this to get a pointer to a memory region into which it can put stuff */
void * shared_memory_ringbuffer_acquire(struct shared_memory_ringbuffer *);

/* and then calls this to actually send it */
void shared_memory_ringbuffer_send(struct shared_memory_ringbuffer * shm, const size_t size);

/* writer calls this to shut it down, indicating to readers that no more data is coming */
void shared_memory_ringbuffer_writer_close(struct shared_memory_ringbuffer * shm);

/* reader functions: */

/* reader calls this to connect to an shm segment. it will return NULL immediately if the
 segment does not exist or is no longer being actively being written to by an alive writer,
 and the reader should react in an application-dependent way. calling code shall also check
 whether this returns MAP_FAILED to indicate an error condition other than the NULL case */
struct shared_memory_ringbuffer_reader * shared_memory_ringbuffer_reader_init(const char * name);

/* reader calls this to get the next packet. it returns 0 immediately if there is no new
 packet, and the reader should react in some application-specific way. -1 is returned if
 there is an error, including in the slow-reader condition */
ssize_t shared_memory_ringbuffer_recv(const void **, struct shared_memory_ringbuffer_reader *);

/* reader should eventually call this upon seeing some application-specific interval in
 which no new packets have arrived, and react by closing down */
int shared_memory_ringbuffer_eof(struct shared_memory_ringbuffer_reader *);

/* reader should call this AFTER doing whatever logic it wants that reads from the most
 recent packet, BEFORE releasing the results of such computation further downstream */
int shared_memory_ringbuffer_reader_has_kept_up(struct shared_memory_ringbuffer_reader *);

/* reader calls this to close down */
void shared_memory_ringbuffer_reader_close(struct shared_memory_ringbuffer_reader * ctx);
