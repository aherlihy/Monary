# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.


MONARY_W_ERRORS_IGNORED = -1
MONARY_W_DEFAULT = -2
MONARY_W_MAJORITY = -3
MONARY_W_TAG = -4


class WriteConcern(object):
    """A python object to mimic the libmongoc mongoc_write_concern_t struct."""
    def __init__(self, w=MONARY_W_DEFAULT, wtimeout=0, wjournal=False,
                 wfsync=False, wtag=None):
        """Create a new WriteConcern.

        The write concern `w` is number of nodes that each document must be
        written to before the server acknowledges the write.

        See the MongoDB manual entry on Write Concern
        (http://docs.mongodb.org/manual/reference/write-concern/) for more
        details.

        :Parameters:
         - `w` (optional): The write concern.
         - `wtimeout` (optional): Number of milliseconds before write timeout.
         - `journal` (optional): Whether or not the write request should be
           journaled before acknowledging the write request.
         - `fsync` (optional): Whether or not fsync() should be called on the
           server before acknowledging the write request.
         - `wtag` (optional): The write concern tag.
        """
        if w < -4:
            raise ValueError("Given 'w' of %d, must be >= -4." % w)

        if wtag is not None and w != MONARY_W_TAG:
            raise ValueError(
                "Cannot specify a tag with 'w' other than MONARY_W_TAG.")

        if wtag is None and w == MONARY_W_TAG:
            raise ValueError(
                "Must specify a tag when 'w' equals MONARY_W_TAG.")
        self.w = w
        self.wtimeout = wtimeout
        self.wjournal = wjournal
        self.wfsync = wfsync
        self.wtag = wtag
        self._c_write_concern = None
        from .monary import cmonary
        self.cmonary = cmonary

    def get_c_write_concern(self):
        """Return a pointer to the C mongoc_write_concern_t struct."""
        if self._c_write_concern is not None:
            self.destroy_c_write_concern()
        self._c_write_concern = self.cmonary.monary_create_write_concern(
            self.w, self.wtimeout, self.wjournal, self.wfsync, self.wtag)
        return self._c_write_concern

    def destroy_c_write_concern(self):
        """Free the C mongoc_write_concern_t struct."""
        if self._c_write_concern is not None:
            self.cmonary.monary_destroy_write_concern(self._c_write_concern)
            self._c_write_concern = None

    def __enter__(self):
        """So WriteConcerns can be cleaned up using with statements."""
        return self

    def __exit__(self, *args):
        """Clean up the C pointer on exit."""
        self.destroy_c_write_concern()

    def __del__(self):
        """Clean up the C pointer on delete."""
        self.destroy_c_write_concern()
