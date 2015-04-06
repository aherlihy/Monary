# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import datetime

MONGO_DATE_EPOCH = datetime.datetime(1970, 1, 1)


def mongodate_to_datetime(mongodate):
    """Converts a Mongo integer date to a datetime instance.

       :param int mongodate: mongo date (represented as milliseconds since
                             January 1, 1970)
       :returns: date represented as a datetime instance
       :rtype: datetime
    """
    return MONGO_DATE_EPOCH + mongodelta_to_timedelta(mongodate)


def datetime_to_mongodate(dt):
    """Converts a datetime instance to a Mongo integer date.

       :param datetime dt: the datetime instance
       :returns: the datetime as an integer date (represented as milliseconds
                 since January 1, 1970)
       :rtype: int
    """
    if not isinstance(dt, (datetime.date, datetime.datetime)):
        raise ValueError("requires a date or datetime value")
    return timedelta_to_mongodelta(dt - MONGO_DATE_EPOCH)


def mongodelta_to_timedelta(mongodelta):
    """Converts a Mongo time difference to a timedelta object.

       :param int mongodelta: the time difference (in milliseconds)
       :returns: a timedelta instance representing the difference
       :rtype: timedelta
    """
    try:
        mongodelta = int(mongodelta)
    except ValueError:
        raise ValueError("time argument must be convertable to integer")
    secs, millis = divmod(mongodelta, 1000)
    return datetime.timedelta(seconds=secs, milliseconds=millis)


def timedelta_to_mongodelta(td):
    """Converts a timedelta instance to a Mongo time difference.

       :param timedelta td: the timedelta instance
       :returns: the time difference in milliseconds
       :rtype: int
    """
    if not isinstance(td, datetime.timedelta):
        raise ValueError("requires a timedelta value")
    millis = (td.microseconds / 1000 +
              (td.seconds + td.days * 24 * 3600) * 1000)
    return millis
