# Monary - Copyright 2011 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo

c = pymongo.Connection("localhost")
c.drop_database("monary_test")
