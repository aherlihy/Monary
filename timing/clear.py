# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import pymongo

if __name__ == '__main__':
    c = pymongo.MongoClient()
    c.drop_database("monary_test")
    print("Database 'monary_test' removed.")
