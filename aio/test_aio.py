import os, sys, time
from twisted.trial import unittest
from twisted.internet import reactor, task
from twisted.internet.threads import deferToThread
from twisted.internet.defer import Deferred
from twisted.python import threadable

TEST_FILENAME = "__test_aio_output__"

class TestAio(unittest.TestCase):

    def setUp(self):
        output = open(TEST_FILENAME, "w")
        output.write("Testing, testing, 123... " * 100)
        output.close()

    def test_segfault(self, *args, **kw):
        """ make sure our beloved C extension doesn't dump core somewhere """
        import _aio
        
        self.assertRaises(IOError, _aio.Queue, -1)
        self.assertRaises(IOError, _aio.Queue, -0)
        self.assertRaises(IOError, _aio.Queue, sys.maxint)

        q = _aio.Queue()
        self.assertEquals(q.processEvents(), None)
        self.assertRaises(IOError, q.processEvents, minEvents = -1, maxEvents = -1, timeoutNSec = -1)
        self.assertRaises(ValueError, q.scheduleRead, 0, 0, 0, 4096)
        self.assertRaises(IOError, q.scheduleRead, -1, 0, 1, 4096)        

        # overflow the queue with too many chunks
        q = _aio.Queue(1)
        fd = os.open(TEST_FILENAME, os.O_RDONLY | os.O_DIRECT)
        q.scheduleRead(fd, 0, 1, 4096)        
        self.assertRaises(_aio.QueueError, q.scheduleRead, fd, 0, 1, 4096)
        os.close(fd)

        
    def test__aio(self, *args, **kw):

        import _aio
        
        q = _aio.Queue()
        reaper = task.LoopingCall(q.processEvents)
        reaper.start(0.1)
        
        fd = os.open(TEST_FILENAME, os.O_RDONLY & os.O_DIRECT)
        
        def _defaultCallback(*args, **kw):
            return True

        def _defaultErrback(*args, **kw):
            return False

        def _shutdown(res):
            self.assertEquals(res, True)
            os.close(fd)
            reaper.stop()

        return q.scheduleRead(fd, 0, 1, 40).addCallbacks(_defaultCallback, _defaultErrback).addCallback(_shutdown)
