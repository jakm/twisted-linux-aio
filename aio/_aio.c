/*

  twisted-linux-aio - asynchronous I/O support for Twisted
  on Linux.

  Copyright (c) 2007 Michal Pasternak <michal.dtz@gmail.com>
  See LICENSE for details.

  Need an implementation for your operating system? Mail me!

*/

#include <Python.h>
#include <structmember.h>
#include <sys/mman.h>
#include <libaio.h>


/* ================================================================================

   Module globals

   ================================================================================ */

static PyObject *QueueError;
static PyObject *Deferred; /* twisted.internet.defer.Deferred */
static PyObject *DeferredList; /* twisted.internet.defer.DeferredList */

int PAGESIZE;

/* End of module globals */

inline PyObject *
aioErrorToException(n) {
    PyObject *args, *exc;
    if (n == -ENOSYS)
      args = Py_BuildValue("(is)", -ENOSYS, "No AIO in kernel.");
    else if (n < 0 && -n < sys_nerr)
      args = Py_BuildValue("(is)", -n, sys_errlist[-n]);
    else
      args = Py_BuildValue("(is)", -n, "Unknown AIO error.");
    exc = PyInstance_New(PyExc_IOError, args, NULL);
    Py_DECREF(args);
    return exc;
}

inline PyObject *
PyErr_SetFromAIOError(n) {
    if (n == -ENOSYS)
      PyErr_SetString(PyExc_IOError, "No AIO in kernel.");
    else if (n < 0 && -n < sys_nerr)
      PyErr_SetString(PyExc_IOError, sys_errlist[-n]);
    else
      PyErr_SetString(PyExc_IOError, "Unknown AIO error");
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
  int maxIO;
  int busy;

  /* private */
  io_context_t *ctx;

} Queue;


static void
Queue_dealloc(Queue* self)
{
  io_queue_release(*self->ctx);
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
    self->ctx = malloc(sizeof(io_context_t));
    if (self->ctx==NULL) {
      Py_DECREF(self);
      return NULL;
    }
    memset(self->ctx, 0, sizeof(io_context_t));
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
  res = io_queue_init(self->maxIO, self->ctx);
  if (res < 0)  {
    PyErr_SetFromAIOError(res);
    return -1;
  }
  return 0;
}

#define Queue_calcAlignedSize(size) (size % PAGESIZE) ?  (size + (PAGESIZE - size % PAGESIZE)) : size

#define Queue_processEvents_CLEANUP { munmap(buf, alignedSize); }



PyObject *
Queue_processEvents(Queue *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[] = {"minEvents", "maxEvents", "timeoutNSec", NULL};
  int minEvents = 1, maxEvents = 16;
  PyObject *ret = NULL;
  struct timespec io_ts;
  io_ts.tv_sec = 0;
  io_ts.tv_nsec = 5000;
  int a, e;

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|iii", kwlist,
				   &minEvents, &maxEvents, &io_ts.tv_nsec))
    return NULL;

  struct io_event events[maxEvents];

  e = io_getevents(*self->ctx, minEvents, maxEvents, &events, &io_ts);

  if (e < 0) 
    return PyErr_SetFromAIOError();

  if (e == 0)
    Py_RETURN_NONE;

  for (a=0;a<e;a++) {
    struct iocb *iocb;
    int iosize, alignedSize;
    char *buf;
    off_t offset;
    PyObject *defer, *arglist, *string;
    int rc, sc, opcode;

    defer = (PyObject *)(events[a].data);

    iocb = events[a].obj;
    iosize = iocb->u.c.nbytes;
    alignedSize = Queue_calcAlignedSize(iosize); /* bytes missing till the end of page */
    buf = iocb->u.c.buf;
    offset = iocb->u.c.offset;
    opcode = iocb->aio_lio_opcode;
    rc = events[a].res2; sc = events[a].res != iosize;

    if (rc || sc) {

      PyObject *errback, *exception;

      errback = PyObject_GetAttrString(defer, "errback");
      if (errback == NULL) { /* Not a Deferred? */
	PyErr_SetString(PyExc_TypeError, "Object passed to Queue.schedule was not a twisted.internet.defer.Deferred object");
	Queue_processEvents_CLEANUP;
	return NULL;
      }

      if (rc) 
	exception = aioErrorToException(rc);
      else if (sc) {
	PyObject *excargs;
	/* iosize = expected; events[a].res = really processed; */
	excargs = Py_BuildValue("(sii)", "Missing bytes", iosize, events[a].res);
	exception = PyInstance_New(PyExc_AssertionError, excargs, NULL);
	Py_DECREF(excargs);
	if (exception == NULL) {	  
	  Queue_processEvents_CLEANUP;
	  return NULL;
	}
      } else {
	fprintf(stderr, "I should never be here.");
	exit(1);
      }

      Queue_processEvents_CLEANUP;
      arglist = Py_BuildValue("(O)", exception);
      ret = PyEval_CallObject(errback, arglist);
      Py_XDECREF(ret);
      Py_DECREF(arglist);
      Py_DECREF(exception);
      Py_DECREF(errback);

    }

    PyObject *callback;

    if (opcode == IO_CMD_PREAD) {
      /*
	 Copy the mmap'ed buffer to a string and pass it to callback.
      */

      string = PyString_FromStringAndSize(buf, iosize);
      Queue_processEvents_CLEANUP;
      arglist = Py_BuildValue("(O)", string);
      callback = PyObject_GetAttrString(defer, "callback");

      if (callback == NULL) { /* Not a Deferred? */
	PyErr_SetString(PyExc_TypeError, "Object passed to Queue.schedule was not a twisted.internet.defer.Deferred object");
	Queue_processEvents_CLEANUP;
	return NULL;
      }

      ret = PyEval_CallObject(callback, arglist);
      Py_XDECREF(ret);
      Py_DECREF(arglist);
      Py_DECREF(callback);
      Py_DECREF(string);

    } else {
      fprintf(stderr, "I should never, ever be here.");
      exit(1);
    }
  }

  Py_RETURN_NONE;
}

#define Queue_scheduleRead_CLEANUP { for(cup=0;cup<a-1;cup++) { munmap(ioq[cup]->u.c.buf, alignedSize); Py_DECREF(deferreds[cup]); } }

static PyObject*
Queue_scheduleRead(Queue *self, PyObject *args, PyObject *kwds) {
  int fd, offset, chunks, chunkSize, a, cup;

  static char *kwlist[] = {"fd", "offset", "chunks", "chunkSize", NULL};

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "iiii|", kwlist,
				   &fd, &offset, &chunks, &chunkSize))

    return NULL;

  if (chunks < 1) {
    PyErr_SetString(PyExc_ValueError, "chunks < 1");
    return NULL;
  } else if ( self->busy + chunks > self->maxIO) { 
    PyErr_SetString(QueueError, "no free slots");
    return NULL;
  }

  struct iocb *ioq[chunks];
  PyObject *deferreds[chunks], *defer, *attrlist, *arglist;
  struct iocb *io;
  char *buf;
  int alignedSize = Queue_calcAlignedSize(chunkSize); /* make sure we want N * PAGESIZE chunks */

  for (a = 0; a < chunks; a++) {
    io = (struct iocb *) malloc(sizeof(struct iocb));
    if (io == NULL) {
      Queue_scheduleRead_CLEANUP;
      return PyErr_NoMemory();
    }

    buf = (char *)mmap(0, alignedSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (buf == NULL)  {
      free(io);
      Queue_scheduleRead_CLEANUP;
      return PyErr_NoMemory();
    }

    // XXX: TODO: CHECK IF THIS WORKS FOR NON-PAGE-ALIGNED SIZES AND OFFSETS!
    // XXX: TODO: (SEE COMMENT ABOUT LIGHTHTTPD BELOW)
    io_prep_pread(io, fd, buf, chunkSize, offset);

    /*
       Create a Deferred object
    */

    attrlist = Py_BuildValue("()");

    defer = PyInstance_New(Deferred, attrlist, NULL);
    if (defer == NULL) {
      munmap(buf, alignedSize);
      Queue_scheduleRead_CLEANUP;
      return NULL;
    }
    Py_DECREF(attrlist);
    io_set_callback(io, defer);
    deferreds[a] = defer;
    ioq[a] = io;

    /*
       lighthttpd sources say something about checking if offset
       is page-aligned... */

    offset += chunkSize;
  }

  self->busy += chunks;
  int res = io_submit(*self->ctx, chunks, ioq);
  if (res < 0)
    return PyErr_SetFromAIOError();

  if (chunks == 1)
    /*
       If there is only one Deferred,
       return it.
    */
    return deferreds[0];

  /*
     Return more, than one Deferred as a DeferredList.
  */
  PyObject *lst = PyList_New(chunks), *dlst;
  for (a = 0; a < chunks; a++)
    PyList_SET_ITEM(lst, a, deferreds[a]);

  arglist = Py_BuildValue("(O)", lst);
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
