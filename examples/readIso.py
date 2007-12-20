#
# twisted-linux-aio proof of concept code
#
# WARNING:
#
# io_submit() will block, if you run out of block layer requests.
#
# We have 128 of those by default, but if your io ends up getting chopped
# into somewhat smaller bits than 1MiB each, then you end up having to
# block on allocation of those. So lets say your /src is mounted on
# /dev/sdaX, try:
#
# echo 512 > /sys/block/sda/queue/nr_requests
#
# -- from http://linux.derkeiler.com/Mailing-Lists/Kernel/2006-11/msg00966.html
#

import os, sys
from twisted.internet import reactor, task
from twisted.python import log
import aio

completed = 0

def _done(data):
    global completed
    sys.stdout.write('OMG!, readed %.2f MB!\n' % (sum([len(x[1]) for x in data if x[0] is True])/(1024.0 * 1024.0)))
    sys.stdout.flush()
    _shutdown()

def _err(*args, **kw):
    sys.stdout.write("ERRBACK!\n%s, %s", (args, kw))

def _shutdown(*args, **kw):
    reactor.stop()

def qAndReaper():
    q = aio.Queue(128)
    reap = task.LoopingCall(q.processEvents, minEvents = 128, maxEvents = 128, timeoutNSec=50)
    reap.start(0.1, now=True)
    return q

def schedule(q):
    fd = os.open("/home/dotz/6.2-RELEASE-i386-disc1.iso", os.O_RDONLY | os.O_DIRECT)
    sys.stdout.write('Scheduling read...\n')
    q.scheduleRead(fd, 0, 128, 409600 * 5).addCallbacks(_done, _err)
    os.close(fd)
    sys.stdout.write('done!\n')
    
log.startLogging(sys.stdout)
annoy = task.LoopingCall(sys.stdout.write, 'PING!\n')
annoy.start(0.1, now=True)
schedule( qAndReaper() )
reactor.run()
