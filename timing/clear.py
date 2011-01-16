import pymongo

c = pymongo.Connection("localhost")
c.drop_database("monary_test")
