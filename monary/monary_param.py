# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

from .monary import get_monary_numpy_type

_SUPPORTED_TYPES = ["bool", "int8", "int16", "int32", "int64",
                    "uint8", "uint16", "uint32", "uint64", "float32",
                    "float64", "date", "id", "timestamp", "string",
                    "binary", "bson"]

_CMONARY_MAX_RECURSION = 100


class MonaryParam(object):
    """An object to be used as a param for Monary insert, remove, and update.
    Each MonaryParam will contain data for a single key in a BSON document.
    """
    def __init__(self, array, field=None, mtype=None):
        """Create a new MonaryParam.

        :Parameters:
         - `array`: Either a tuple/list of (masked_array, field [, type]) or a
           masked_array. The values in the masked_array will be used as the
           values for the resulting BSON documents when passed to Monary.
         - `field` (optional): Only optional when specified in `array`. This
           will be used as the field name.
         - `mtype` (optional): The Monary type that corresponds to the numpy
           dtype of the array. This is require for the following types: binary,
           bson, id, datetime, timestamp, and string.
        """
        if field is None:
            if mtype is not None:
                raise ValueError("If a type is specified, a field name"
                                 " must be specified too.")
            if len(array) not in [2, 3]:
                raise ValueError("If type and field name are left empty, the"
                                 " first parameter must be able to be unpacked"
                                 " into ``array, field [, type]``.")
            if len(array) == 2:
                array, field = array
                mtype = str(array.data.dtype)
            else:
                array, field, mtype = array
        elif field == "":
            raise ValueError("Field name must not be empty.")
        elif field.count(".") >= _CMONARY_MAX_RECURSION:
            raise ValueError(
                "Fields name %r exceeds max nested document level (%d)." %
                (field, _CMONARY_MAX_RECURSION))
        elif mtype is None:
            mtype = str(array.data.dtype)

        if mtype.split(":")[0] not in _SUPPORTED_TYPES:
            raise ValueError("MonaryParam cannot be of type %r." % mtype)

        self.array, self.field, self.mtype = array, field, mtype

        m_np_t = get_monary_numpy_type(self.mtype)
        self.cmonary_type, self.cmonary_type_arg, self.numpy_type = m_np_t

        if self.array.data.dtype != self.numpy_type:
            raise ValueError("Wrong type specified: given %r expected %r." %
                             (self.array.data.dtype, self.numpy_type))

    @classmethod
    def from_lists(cls, data, fields, types=None):
        """Create a list of MonaryParams from lists of arguments. These three
        lists will be repeated passed to __init__ as array, field, and mtype.

        :Parameters:
         - `data`: List of numpy masked arrays.
         - `fields`: List of field name.
         - `types` (optional): List of corresponding types.
        """
        if types is None:
            if not (len(data) == len(fields)):
                raise ValueError(
                    "Data and fields must be of equal length.")
            return cls.from_groups(zip(data, fields))
        else:
            if not (len(data) == len(fields) == len(types)):
                raise ValueError(
                    "Data, fields, and types must be of equal length.")
            return cls.from_groups(zip(data, fields, types))

    @classmethod
    def from_groups(cls, groups):
        """Create a list of MonaryParams from lists of lists/tuples. Each item
        in the group list will be passed to __init__ as array; field and mtype
        will be left as None, and __init__ will unpack the values.

        :Parameters:
         - `groups`: List of items to be passed to MonaryParam.
        """
        return list(map(lambda x: cls(x), groups))

    def __len__(self):
        """Return the length of the masked array."""
        return len(self.array)

    def __getitem__(self, key):
        """Return the item at the given index of the masked array.

        :Parameters:
         - `key`: A key than can be used to access an item in a masked_array.
        """
        return self.array[key]
