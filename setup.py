# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.
import os
import pkgconfig
import platform
import sys
import warnings

# Don't force people to install setuptools unless
# we have to.
try:
    from setuptools import setup, Extension
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, Extension


mongoc_src = None
bson_src = None
for s in range(len(sys.argv) - 1, -1, -1):
    if sys.argv[s] == "--libmongoc-root":
        mongoc_src = sys.argv[s+1]
	sys.argv.remove("--libbson-root")
	sys.argv.remove(bson_src)


    if sys.argv[s] == "--libbson-root":
        bson_src = sys.argv[s+1]
	sys.argv.remove("--libmongoc-root")
	sys.argv.remove(mongoc_src)


test_requires = []
test_suite = "test"
if sys.version_info[:2] == (2, 6):
    test_requires.append("unittest2")
    test_suite = "unittest2.collector"

DEBUG = False

VERSION = "0.3.0"

CFLAGS = ["-fPIC", "-O2"]
settings = {}

if not DEBUG:
    CFLAGS.append("-DNDEBUG")

class BuildException(Exception):
    """Indicates an error occurred while compiling from source."""
    pass


settings['export_symbols'] = ["monary_init",
                              "monary_cleanup",
                              "monary_connect",
                              "monary_disconnect",
                              "monary_use_collection",
                              "monary_destroy_collection",
                              "monary_alloc_column_data",
                              "monary_free_column_data",
                              "monary_set_column_item",
                              "monary_query_count",
                              "monary_init_query",
                              "monary_init_aggregate",
                              "monary_load_query",
                              "monary_close_query",
                              "monary_create_write_concern",
                              "monary_destroy_write_concern",
                              "monary_insert"]

settings['sources'] = [os.path.join("monary", "cmonary.c")]

try:
    if pkgconfig.exists("libmongoc-1.0"):
        pkgcfg = pkgconfig.parse("libmongoc-1.0")
        settings['include_dirs'] = list(pkgcfg['include_dirs'])
        settings['library_dirs'] = list(pkgcfg['library_dirs'])
        settings['libraries'] = list(pkgcfg['libraries'])
        settings['define_macros'] = list(pkgcfg['define_macros'])
    else:
        raise BuildException("Error, unable to find libmongoc-1.0"
                                 " with pkgconfig")
except EnvironmentError as e:
    if platform.system() == 'Windows':
        if mongoc_src is None and bson_src is None:
            warnings.warn("Warning: no prefix given for libmongoc. "
                      "Defaulting to C:\\Program Files. To specify, please "
                      "call setup.py with the following arguments:\n"
                      "\tpython setup.py install --libbson-root C://usr --libmongoc-root C://usr\n")
            # Set default location for libmongoc and libbson.
            mongoc_src = os.path.join("C:/", "Program Files")
            bson_src = os.path.join("C:/", "Program Files")

        # Search command line args for libmongoc and libbson path.
        

        settings["libraries"] = ["bson-1.0", "mongoc-1.0"]
        settings['include_dirs'] = [os.path.join(mongoc_src,
                                                     "include",
                                                     "libmongoc-1.0"),
                                        os.path.join(bson_src,
                                                     "include",
                                                     "libbson-1.0")]
        settings['library_dirs'] = [os.path.join(mongoc_src,
                                                     "lib"),
                                        os.path.join(bson_src,
                                                     "lib")]
    else:
        raise BuildException("Error in pkgconfig: ", e)


module = Extension('monary.libcmonary',
                   extra_compile_args=CFLAGS,
                   include_dirs=settings['include_dirs'],
                   libraries=settings['libraries'],
                   library_dirs=settings['library_dirs'],
                   export_symbols=settings['export_symbols'],
                   sources=settings['sources'])



# Get README info
try:
    with open("README.rst") as fd:
        readme_content = fd.read()
except:
    readme_content = ""

setup(
    name = "Monary",
    version = VERSION,
    packages = ["monary"],
    setup_requires = ["pymongo", "numpy"],
    tests_require = test_requires,
    package_dir = {"monary": "monary"},

    ext_modules = [module],

    author = "David J. C. Beach",
    author_email = "info@djcinnovations.com",
    description = "Monary performs high-performance column queries from MongoDB.",
    long_description = readme_content,
    keywords = "monary pymongo mongo mongodb numpy array",
    classifiers = [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Unix",
        "Operating System :: Microsoft :: Windows",
        "Environment :: Console",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database"
    ],
    url = "http://bitbucket.org/djcbeach/monary/",
    test_suite = test_suite,
)
