# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import datetime

from monary.datehelper import datetime_to_mongodate, mongodate_to_datetime

DT = datetime.datetime

DATES = [
    (DT(1970, 1, 1),                0),
    (DT(1970, 1, 1, 0, 0, 10),      10 * 1000),
    (DT(1971, 1, 1),                365 * 24 * 60 * 60 * 1000),
]

def test_datetime_to_mongo():
    for dt, mongo in DATES:
        assert datetime_to_mongodate(dt) == mongo

def test_mongo_to_datetime():
    for dt, mongo in DATES:
        assert mongodate_to_datetime(mongo) == dt
