/*
 *  libasyio - simple layer to use KAIO + eventfd
 *  Copyright (C) 2007  Michal Pasternak <michal.dtz@gmail.com>
 *
 *  Code taken from eventfd-aio-test.
 *
 *  eventfd-aio-test by Davide Libenzi (test app for eventfd hooked into KAIO)
 *  Copyright (C) 2007  Davide Libenzi
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

#define _GNU_SOURCE
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/signal.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <poll.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>

typedef unsigned long aio_context_t;

enum {
  IOCB_CMD_PREAD = 0,
  IOCB_CMD_PWRITE = 1,
  IOCB_CMD_FSYNC = 2,
  IOCB_CMD_FDSYNC = 3,
  /* These two are experimental.
   * IOCB_CMD_PREADX = 4,
   * IOCB_CMD_POLL = 5,
   */
  IOCB_CMD_NOOP = 6,
  IOCB_CMD_PREADV = 7,
  IOCB_CMD_PWRITEV = 8,
};

#if defined(__LITTLE_ENDIAN)
# define PADDED(x,y)	x, y
#elif defined(__BIG_ENDIAN)
# define PADDED(x,y)	y, x
#else
# error edit for your odd byteorder.
#endif

#define IOCB_FLAG_RESFD		(1 << 0)

/*
 * we always use a 64bit off_t when communicating
 * with userland.  its up to libraries to do the
 * proper padding and aio_error abstraction
 */
struct iocb {
  /* these are internal to the kernel/libc. */
  u_int64_t	aio_data;	/* data to be returned in event's data */
  u_int32_t	PADDED(aio_key, aio_reserved1);
  /* the kernel sets aio_key to the req # */
  
  /* common fields */
  u_int16_t	aio_lio_opcode;	/* see IOCB_CMD_ above */
  int16_t	aio_reqprio;
  u_int32_t	aio_fildes;
  
  u_int64_t	aio_buf;
  u_int64_t	aio_nbytes;
  int64_t	aio_offset;
  
  /* extra parameters */
  u_int64_t	aio_reserved2;	/* TODO: use this for a (struct sigevent *) */
  
  u_int32_t	aio_flags;
  /*
   * If different from 0, this is an eventfd to deliver AIO results to
   */
  u_int32_t	aio_resfd;
}; /* 64 bytes */

struct io_event {
  u_int64_t 		data;           /* the data field from the iocb */
  u_int64_t 		obj;            /* what iocb this event came from */
  int64_t 		res;            /* result code for this event */
  int64_t 		res2;           /* secondary result */
};

inline void asyio_prep_pread(struct iocb *iocb, int fd, void *buf, int nr_segs,
		      int64_t offset, int afd) {
  memset(iocb, 0, sizeof(*iocb));
  iocb->aio_fildes = fd;
  iocb->aio_lio_opcode = IOCB_CMD_PREAD;
  iocb->aio_reqprio = 0;
  iocb->aio_buf = (u_int64_t) buf;
  iocb->aio_nbytes = nr_segs;
  iocb->aio_offset = offset;
  iocb->aio_flags = IOCB_FLAG_RESFD;
  iocb->aio_resfd = afd;
}

inline void asyio_prep_pwrite(struct iocb *iocb, int fd, void const *buf, int nr_segs,
		       int64_t offset, int afd) {
  memset(iocb, 0, sizeof(*iocb));
  iocb->aio_fildes = fd;
  iocb->aio_lio_opcode = IOCB_CMD_PWRITE;
  iocb->aio_reqprio = 0;
  iocb->aio_buf = (u_int64_t) buf;
  iocb->aio_nbytes = nr_segs;
  iocb->aio_offset = offset;
  iocb->aio_flags = IOCB_FLAG_RESFD;
  iocb->aio_resfd = afd;
}

inline long io_setup(unsigned nr_reqs, aio_context_t *ctx) {
  return syscall(__NR_io_setup, nr_reqs, ctx);
}

inline long io_destroy(aio_context_t ctx) {
  return syscall(__NR_io_destroy, ctx);
}

inline long io_submit(aio_context_t ctx, long n, struct iocb **paiocb) {
  return syscall(__NR_io_submit, ctx, n, paiocb);
}

inline long io_cancel(aio_context_t ctx, struct iocb *aiocb, struct io_event *res) {
  return syscall(__NR_io_cancel, ctx, aiocb, res);
}

inline long io_getevents(aio_context_t ctx, long min_nr, long nr, struct io_event *events,
		  struct timespec *tmo) {
  return syscall(__NR_io_getevents, ctx, min_nr, nr, events, tmo);
}

inline void io_set_callback(struct iocb *iocb, u_int64_t cb) {
  iocb->aio_data = (void *)cb;
}

inline int eventfd(int count) {
  return syscall(__NR_eventfd, count);
}
