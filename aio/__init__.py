import os, stat, time, sys

from twisted.internet import reactor, abstract, defer

from _aio import Queue as _aio_Queue, QueueError

class KAIOFd(abstract.FileDescriptor):
    """
    This is a layer connecting fd created by eventfd call
    with Twisted's reactor (preferably poll or epoll).

    self.fd is eventfd filedescriptor.
    
    self.fd is available to read, when there are KAIO events
    waiting to be processed.
    
    Read self.fd and get the number of events waiting.

    """

    def __init__(self, fd, queue):
        abstract.FileDescriptor.__init__(self)
        self.fd = fd
        self.queue = queue
    def fileno(self):
        return self.fd
    def doRead(self):
        buf = os.read(self.fd, 4096)
        noEvents = 0
        shl = 0
        for a in buf:
            noEvents += ord(a) << shl
            shl += 8
        return self.queue.processEvents(minEvents = noEvents, maxEvents = noEvents, timeoutNSec = 1)
    # return

class KAIOCooperator(object):
    """This is an object, which cooperates with aio.Queue.    
    """

    whenQueueFullDelay = 0.01

    def __init__(self, queue, chunks):
        self.queue = queue
        self.chunks = chunks
        self.completed = 0

    def start(self):
        return self.queueMe()

    def completed(self):
        raise Exception(NotImplemented)

    def allowedToQueue(self, noSlots):
        raise Exception(NotImplemented)
    
    def chunkCollected(self, data):
        raise Exception(NotImplemented)

    def error(self, *args):
        print "ERROR"
        print str(args[0])
        sys.exit(-1)
    
    def queueMe(self):
        chunksLeft = self.chunks - self.completed
        if chunksLeft < 1:
            return self.completed()
        availableSlots = self.queue.maxIO - self.queue.busy
        if availableSlots < 1:
            return reactor.callLater(self.whenQueueFullDelay, self.queueMe)
        thisTurn = chunksLeft
        if thisTurn > availableSlots:
            thisTurn = availableSlots  # XXX: shouldn't we take a parametrized slice of queue instead of just whole queue here?
        d = self.allowedToQueue(noSlots = thisTurn)
        d.addCallbacks(self.chunkCollected).addCallback(lambda x, y: reactor.callLater, self.queueMe)
        d.addErrback(self.error)
        return d
        
class DeferredFile(KAIOCooperator):
    """This is DeferredFile, a file which is read in asynchronous way via KAIO.
    """
    def __init__(self, queue, filename, callback = None, chunkSize = 4096):
        self.filename = filename
        self.fd = os.open(filename, os.O_RDONLY | os.O_DIRECT)
        self.fileSize = os.stat(filename).st_size
        self.chunkSize = chunkSize
        chunks = self.fileSize / self.chunkSize
        if self.fileSize % self.chunkSize:
            chunks += 1
        KAIOCooperator.__init__(self, queue, chunks)
        self.defer = defer.Deferred()

    def allowedToQueue(self, noSlots):
        return self.queue.scheduleRead(self.fd, self.completed * self.chunkSize, noSlots, self.chunkSize)

    def chunkCollected(self, data):
        # data chunks are now ignored
        pass

    def completed(self):
        return self.defer.callback()

class Queue(_aio_Queue):
    def __init__(self, *args, **kw):
        _aio_Queue.__init__(self, *args, **kw)
        self.reader = KAIOFd(self.fd, self)
        reactor.addReader(self.reader)

    def readfile(self, filename, chunkSize = 4096, callback=None):
        f = DeferredFile(self, filename, chunkSize, callback)
        return f.defer
        
        
