Binary Data Example
===================

This example shows you how to obtain blocks of binary data from MongoDB with
Monary.

Setup
-----

We are going to use 100 random files for this example.
If you don't happen to have random files lying around, you can issue this
command at a Unix shell:

.. code-block:: bash

    $ for ((i = 0; i < 100; i=i+1)); do
    > head -c $SIZE < /dev/urandom > "img${i}.jpg"
    > done

This creates 100 files, each containing ``$SIZE`` bytes of random data.


For this example, let's use Monary to insert raw binary image data into
MongoDB. First, we can set up a connection to the local MongoDB database::

    >>> from monary import Monary
    >>> client = Monary()


Next, we open some random image files. Assume we have image files named
``img0.jpg`` through ``img99.jpg``::

    >>> import numpy as np
    >>> from numpy import ma
    >>> images = []
    >>> sizes = ma.masked_array(np.zeros(100, dtype="uint32"),
    ...                         np.zeros(100, dtype="bool"))
    >>> for i in range(0, 100):
    ...     with open("img%d.jpg" % i, "rb") as img:
    ...         f = img.read()
    ...         images.append(f)
    ...         sizes[i] = len(f)

Next we convert the image list into a numpy masked array:

    >>> max_size = sizes.max()
    >>> img_type = "<V%d" % max_size
    >>> img_array = ma.masked_array(np.array(images, dtype=img_type),
    ...                             np.zeros(100, dtype="bool"))

Finally, we use Monary's ``binary`` type to insert the data into MongoDB::

    >>> from monary import MonaryParam
    >>> img_mp = MonaryParam(img_array, "img", "binary:%d" % max_size)
    >>> size_mp = MonaryParam(sizes, "size")
    >>> client.insert("test", "data", [img_mp, size_mp])

.. seealso::

    :doc:`The MonaryParam Example </examples/monary-param>` and
    :doc:`The Monary Insert Example </examples/insert>`


.. note::

     We also store the original file size. This is because ``img_array`` has a
     fixed width, so all of the images, once put into MongoDB, will be of size
     ``max_size``. When we retrieve this data, it will be useful to know the
     original file size so we can truncate the binary data appropriately.


Finding Binary Data
-------------------
To query binary data, Monary requires the size of the binary to load in. Since
the data in different documents can be of different sizes, we need to use the
size of the biggest binary blob to avoid truncation.

To find the size of the data in bytes, we use the ``size`` type::

    >>> sizes, = client.query("test", "data", {}, ["img"], ["size"])
    >>> sizes
    masked_array(data = [255L 255L 255L ..., 255L 255L 255L],
                 mask = [False False False ..., False False False],
           fill_value = 999999)

Note that these sizes are unsigned 32-bit integers::

    >>> sizes[0]
    255
    >>> type(sizes[0])
    <type 'numpy.uint32'>

We can get the maximum image size by calling ``max``::

    >>> max_size = sizes.max()

Finally, we can issue a query command to get pointers to the binary data::

    >>> data, sizes = client.query("test", "data", {},
    ...                            ["img", "size"],
    ...                            ["binary:%d" % max_size, "uint32"])
    >>> data
    masked_array(data = [<read-write buffer ptr 0x7f8a58421b50, size 255 at 0x105b6deb0> ...],
                 mask = [False ...]
           fill_value = ???)

Each buffer pointer is of type ``numpy.ma.core.mvoid``. From here, you can use
NumPy to manipulate the data or export it to another location.
