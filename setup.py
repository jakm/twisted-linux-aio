from distutils.core import setup
from distutils.extension import Extension

setup(name = "twisted-linux-aio",
      version = "0.1",
      maintainer = "Michal Pasternak",
      maintainer_email = "michal.dtz@gmail.com",
      description = "Integration of Twisted with asynchronous I/O layer on Linux",
      url = "http://twisted-linux-aio.google.com/",
      platforms = "linux",
      license = "MIT",
      packages = [ 'aio' ], 
      ext_modules = [ Extension( "_aio", ["aio/_aio.c"], libraries = ["aio"] ) ] )
                      
