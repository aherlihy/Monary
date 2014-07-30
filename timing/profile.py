# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

from time import time

class profile(object):
    
    def __init__(self, name):
        self._name = name
        
    def __enter__(self):
        self._start = time()
        
    def __exit__(self, *args):
        stop = time()
        print("%s took %6.2f s" % (self._name, stop - self._start))
