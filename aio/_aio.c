/*

  twisted-linux-aio - asynchronous I/O support for Twisted
  on Linux.

  Copyright (c) 2007 Michal Pasternak <michal.dtz@gmail.com>
  See LICENSE for details.

*/

#include <Python.h>
#include <structmember.h>
#include <sys/mman.h>

#include "libasyio.c"

/* ================================================================================

   Module globals

   ================================================================================ */

static PyObject *QueueError;
static PyObject *Deferred; /* twisted.internet.defer.Deferred */
static PyObject *DeferredList; /* twisted.internet.defer.DeferredList */

int PAGESIZE;

/* End of module globals */

PyObject *
PyErr_SetFromAIOError(int n) {
    if (n == -ENOSYS)
      return PyErr_Format(PyExc_IOError, "%s", "No AIO in kernel.");
    else if (n < 0 && -n < sys_nerr)
      return PyErr_Format(PyExc_IOError, "%s", sys_errlist[-n]);
    else
      return PyErr_Format(PyExc_IOError, "%s", "Unknown AIO error");
    return NULL;
}


/* ================================================================================

  _aio.Queue

   This is python-aio Queue, capable of receiving maxIO events.

   It also can schedule operations and return completed events.

   ================================================================================ */

typedef struct {
  PyObject_HEAD

  /* public */
  unsigned int maxIO; /* maximum number of handled events */
  unsigned int busy; /* current handled events */
  unsigned int fd; /* notification fd */

  /* private */
  aio_context_t *ctx;

  struct iocb *iocbs;

} Queue;


static void
Queue_dealloc(Queue* self)
{
  io_destroy(*self->ctx);
  self->ob_type->tp_free((PyObject*)self);
}

static PyObject *
Queue_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
  Queue *self;

  self = (Queue *)type->tp_alloc(type, 0);
  if (self != NULL) {
    self->maxIO = 32;
    self->busy = 0;
    self->fd = -1;
    self->ctx = malloc(sizeof(aio_context_t));
    if (self->ctx==NULL) {
      Py_DECREF(self);
      return NULL;
    }
    memset(self->ctx, 0, sizeof(aio_context_t));
  }

  return (PyObject *)self;
}

static int
Queue_init(Queue *self, PyObject *args, PyObject *kwds)
{
  int res;
  static char *kwlist[] = {"maxIO", NULL};

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i", kwlist, &self->maxIO))
    return -1;

  res = io_setup(self->maxIO, self->ctx);
  if (res < 0)  {
    PyErr_SetFromAIOError(res);
    return -1;
  }

  self->fd = eventfd(0);
  if (self->fd == -1) {
    PyErr_SetFromErrno(PyExc_IOError);
    return -1;
  }
  fcntl(self->fd, F_SETFL, fcntl(self->fd, F_GETFL, 0) | O_NONBLOCK);

  return 0;
}

#define Queue_calcAlignedSize(size) (size % PAGESIZE) ?  (size + (PAGESIZE - size % PAGESIZE)) : size

#define Queue_processEvents_CLEANUP {			\
    if (iocb) free(iocb); if (buf) free(buf);		\
    Py_XDECREF(defer);					\
    for (cup=a+1;cup<e;cup++) {				\
      iocb = (struct iocb *)events[a].obj;		\
      defer = (PyObject *)iocb->aio_data;		\
      buf = (char *)iocb->aio_buf;			\
      free(iocb); free(buf); Py_XDECREF(defer);		\
    }							\
  }

PyObject *
Queue_processEvents(Queue *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[] = {"minEvents", "maxEvents", "timeoutNSec", NULL};
  int minEvents = 1, maxEvents = 16;
  PyObject *ret = NULL;
  struct timespec io_ts;
  io_ts.tv_sec = 0;
  io_ts.tv_nsec = 5000;
  int a, e, cup;
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|iii", kwlist,
				   &minEvents, &maxEvents, &io_ts.tv_nsec))
    return NULL;

  struct io_event events[maxEvents];
  e = io_getevents(*self->ctx, minEvents, maxEvents, events, &io_ts);
  if (e < 0) {
    PyErr_SetFromAIOError(e);
    return NULL;
  }

  if (e == 0)
    Py_RETURN_NONE;

  for (a=0;a<e;a++) {
    struct iocb *iocb;
    uint iosize, alignedSize;
    char *buf;
    off_t offset;
    PyObject *defer, *arglist, *string;
    int rc, sc, opcode;

    iocb = (struct iocb *)events[a].obj;
    defer = (PyObject *)iocb->aio_data;
    iosize = iocb->aio_nbytes;
    alignedSize = Queue_calcAlignedSize(iosize); /* bytes missing till the end of page */
    buf = (char *)iocb->aio_buf;
    offset = iocb->aio_offset;
    opcode = iocb->aio_lio_opcode;
    rc = events[a].res2; sc = events[a].res != iosize;

    if (rc || sc) {

      PyObject *errback, *exception;
      
      errback = PyObject_GetAttrString(defer, "errback");
      if (errback == NULL) { /* Not a Deferred? */
	PyErr_SetString(PyExc_TypeError, "Object passed to Queue.schedule was not a twisted.internet.defer.Deferred object (no errback attribute).");
	Queue_processEvents_CLEANUP;
	return NULL;
      }

      if (rc) 
	exception = PyErr_SetFromAIOError(rc);
      else if (sc) {
	PyObject *excargs;
	/* iosize = expected; events[a].res = really processed; */
	exception = PyErr_Format(PyExc_AssertionError, "Missing bytes: should read %i, got %i.", iosize, events[a].res);
	if (exception == NULL) {	  
	  Queue_processEvents_CLEANUP;
	  return NULL;
	}
      } else {
	fprintf(stderr, "I should never be here.");
	exit(1);
      }
      
      free(buf); free(iocb);
      arglist = Py_BuildValue("(O)", exception);
      ret = PyEval_CallObject(errback, arglist);
      Py_DECREF(arglist);
      Py_DECREF(exception);
      Py_DECREF(errback);
      if (ret == NULL) {
	Queue_processEvents_CLEANUP;	
	return NULL;
      }
      Py_XDECREF(ret);
    }
    PyObject *callback;

    if (opcode == IOCB_CMD_PREAD) {
      /*
	 Copy the buffer to a string and pass it to callback.
      */
      string = PyString_FromStringAndSize(buf, iosize);
      free(buf); 

      arglist = Py_BuildValue("(O)", string);
      if (defer == NULL) {
	PyErr_SetString(PyExc_TypeError, "aio_data is NULL");
	Py_XDECREF(string);
	Queue_processEvents_CLEANUP;
	return NULL;
      }
      callback = PyObject_GetAttrString(defer, "callback");
      if (callback == NULL) { /* Not a Deferred? */
	PyErr_SetString(PyExc_TypeError, "Object passed to Queue.schedule was not a twisted.internet.defer.Deferred object (no callback attribute).");
	Queue_processEvents_CLEANUP;
	return NULL;
      }
      ret = PyEval_CallObject(callback, arglist);
      Py_DECREF(arglist);
      Py_DECREF(callback);
      Py_DECREF(string);
      Py_DECREF(defer);
      
      if (ret == NULL)
	return NULL;
      Py_XDECREF(ret);
    } else {
      fprintf(stderr, "I should never, ever be here.");
      exit(1);
    }
  }

  self->busy -= e;

  Py_RETURN_NONE;
}

#define Queue_scheduleRead_CLEANUP { for(cup=0;cup< a>0?a-1:0;cup++) { printf("TOTAL: %i, NOW: %i, TO: %i\n", chunks, cup, a); \
      if (ioq[cup]->aio_buf) free(ioq[cup]->aio_buf); \
      Py_XDECREF(deferreds[cup]); } if (ioq) free(ioq); if (deferreds) free(deferreds); }

static PyObject*
Queue_scheduleRead(Queue *self, PyObject *args, PyObject *kwds) {
  unsigned int fd, offset, chunks, chunkSize, a, cup;
  static char *kwlist[] = {"fd", "offset", "chunks", "chunkSize", NULL};

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "iiii|", kwlist,
				   &fd, &offset, &chunks, &chunkSize))
    return NULL;

  if ( self->busy + chunks > self->maxIO ) { 
    PyErr_SetString(QueueError, "can not accept new schedules - no free slots");
    return NULL;
  }  else if ( chunks < 1 ) {
    PyErr_SetString(PyExc_IOError, "chunks < 1");
    return NULL;
  }

  struct iocb *ioq[chunks];
  PyObject *deferreds[chunks];
  PyObject *defer, *attrlist;

  for (a=0;a<chunks;a++) {
    attrlist = Py_BuildValue("()");
    deferreds[a] = PyInstance_New(Deferred, attrlist, NULL);
    if (deferreds[a] == NULL) {
      Queue_scheduleRead_CLEANUP;
      return NULL;
    }
    Py_DECREF(attrlist);
  }
  int alignedSize = Queue_calcAlignedSize(chunkSize); /* make sure we want N * PAGESIZE chunks */

  char *buf ;
  struct iocb *io;

  for (a = 0; a < chunks; a++) {

    buf = valloc(alignedSize);
    if (buf == NULL)  {
      Queue_scheduleRead_CLEANUP;
      return PyErr_NoMemory();
    }
    
    io = malloc(sizeof(struct iocb));
    if (io == NULL) {
      Queue_scheduleRead_CLEANUP;
      return PyErr_NoMemory();
    }

    asyio_prep_pread(io, fd, buf, chunkSize, offset, self->fd);
    io->aio_data = deferreds[a];
    ioq[a] = io;
    offset += chunkSize;
  }
  self->busy += chunks;
  int res = io_submit(*self->ctx, chunks, ioq);
  if (res < 0) {
    PyErr_SetFromAIOError(res);
    return NULL;
  }

  PyObject *lst, *dlst, *arglist;
  lst = PyList_New(chunks);
  for (a = 0; a < chunks; a++) {
    PyList_SET_ITEM(lst, a, deferreds[a]); 
    Py_INCREF(deferreds[a]);
  }
  arglist = Py_BuildValue("(O)", lst);
  if (arglist == NULL)
    return PyErr_NoMemory();
  dlst = PyInstance_New(DeferredList, arglist, NULL);
  Py_DECREF(arglist);
  return dlst; 
}

static PyMemberDef Queue_members[] = {
  {"maxIO", T_INT, offsetof(Queue, maxIO), 0,
   "Maximum number of simultaneous asynchronous operations\n\
this object can handle. See man:io_queue_init(2) ."},
  {"busy", T_INT, offsetof(Queue, busy), 0,
   "Number of currently handled operations."},
  {"fd", T_INT, offsetof(Queue, fd), 0,
   "Filedescriptor, which will receive notification events.\n\
See: man:eventfd(2) ."},
  {NULL}  /* Sentinel */
};

static PyMethodDef Queue_methods[] = {

  {"processEvents", (PyCFunction)Queue_processEvents, METH_VARARGS|METH_KEYWORDS,
   "processEvents(minEvents, maxEvents, timeoutNSec)\n\
 -- receive at least minEvents in timeoutNSec time.\n\
\n\
This method actually processes events and calls callbacks \n\
and errbacks accordingly. \n\
\n\
@returns: None\n\
See man:io_getevents(2) ."},

  {"scheduleRead", (PyCFunction)Queue_scheduleRead, METH_VARARGS|METH_KEYWORDS, "scheduleRead(fd, offset, chunks, chunksSize);\n\
 -- schedule a read operation on filedescriptor fd,\n\
 starting with offset, dividing the operation to \n\
 no. chunks, each as long as chunkSize.\n\
\n\
@returns: twisted.internet.defer.Deferred object if only one chunk \n\
or twisted.internet.defer.DeferredList if many chunks.\n\
\n\
See man:io_prep_pread(2) .\n"},
  {NULL, NULL, 0, NULL}
};

static PyTypeObject QueueType = {
  PyObject_HEAD_INIT(NULL)
  0,                         /*ob_size*/
  "_aio.Queue",              /*tp_name*/
  sizeof(Queue),             /*tp_basicsize*/
  0,                         /*tp_itemsize*/
  (destructor)Queue_dealloc, /*tp_dealloc*/
  0,                         /*tp_print*/
  0,                         /*tp_getattr*/
  0,                         /*tp_setattr*/
  0,                         /*tp_compare*/
  0,                         /*tp_repr*/
  0,                         /*tp_as_number*/
  0,                         /*tp_as_sequence*/
  0,                         /*tp_as_mapping*/
  0,                         /*tp_hash */
  0,                         /*tp_call*/
  0,                         /*tp_str*/
  0,                         /*tp_getattro*/
  0,                         /*tp_setattro*/
  0,                         /*tp_as_buffer*/
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
  "Queue objects",           /* tp_doc */
  0,		               /* tp_traverse */
  0,		               /* tp_clear */
  0,		               /* tp_richcompare */
  0,		               /* tp_weaklistoffset */
  0,		               /* tp_iter */
  0,		               /* tp_iternext */
  Queue_methods,             /* tp_methods */
  Queue_members,             /* tp_members */
  0,                         /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)Queue_init,      /* tp_init */
  0,                         /* tp_alloc */
  Queue_new,                 /* tp_new */
};


/* ============================== END OF _aio.Queue ======================================== */


static PyMethodDef module_methods[] = {
  {NULL}  /* Sentinel */
};

#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC
init_aio(void)
{
  PyObject* m;

  PAGESIZE = sysconf(_SC_PAGESIZE);

  if (PyType_Ready(&QueueType) < 0)
    return;

  m = Py_InitModule3("_aio", module_methods, "libaio wrapper.");
  if (m == NULL)
    return;

  Py_INCREF(&QueueType);
  PyModule_AddObject(m, "Queue", (PyObject *)&QueueType);

  QueueError = PyErr_NewException("_aio.QueueError", NULL, NULL);
  Py_INCREF(QueueError);
  PyModule_AddObject(m, "QueueError", QueueError);

  PyObject *defer;
  defer = PyImport_ImportModule("twisted.internet.defer");
  if (defer == NULL)
    return;

  Deferred = PyObject_GetAttrString(defer, "Deferred");
  if (Deferred == NULL) {
    PyErr_SetString(PyExc_ImportError, "Can not import twisted.internet.defer.Deferred.");
    return;
  }

  DeferredList = PyObject_GetAttrString(defer, "DeferredList");
  if (Deferred == NULL) {
    PyErr_SetString(PyExc_ImportError, "Can not import twisted.internet.defer.DeferredList.");
    return;
  }
}
