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
from twisted.internet import epollreactor
epollreactor.install()
from twisted.internet import task, reactor
from twisted.python import log
import aio

def _done(data):
    sys.stdout.write('OMG!, readed %.2f MB!\n' % (sum([len(x[1]) for x in data if x[0] is True])/(1024.0 * 1024.0)))
    sys.stdout.flush()
    _shutdown()

def _err(*args, **kw):
    sys.stdout.write("ERRBACK!\n%s, %s", (args, kw))

def _shutdown(*args, **kw):
    reactor.stop()

def _prepare():
    task.LoopingCall(sys.stdout.write, 'PING! Just a annoying reminder\n').start(0.5, now=False)
    q = aio.Queue()
    df = aio.DeferredFile(q, "/home/dotz/6.2-RELEASE-i386-disc1.iso")
    #task.LoopingCall(df.stats).start(5, now=False)
    df.defer.addCallbacks(_done, _err)
    df.start()
    return df.defer
    
log.startLogging(sys.stdout)
_prepare()
reactor.run()
