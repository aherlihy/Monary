# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.
import os
import platform
import sys
import warnings

# Don't force people to install setuptools unless we have to.
try:
    from setuptools import setup, Extension
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, Extension

# Set the default location of libmongoc and libbson.
# NOTE: Will only use the default if pkgconfig cannot find anything.
if platform.system() == "Windows":
    mongoc_src = os.path.join("C:/", "Program Files", "libmongoc")
    bson_src = os.path.join("C:/", "Program Files", "libbson")
    libraries = ["bson-1.0", "mongoc-1.0"]
else:
    mongoc_src = os.path.join("/usr", "local")
    bson_src = os.path.join("/usr", "local")
    # Libmongoc MUST be compiled with SSL and SASL.
    libraries = ["bson-1.0", "crypto", "ssl", "sasl2", "mongoc-1.0"]

# Check if the user specified the location.
for s in range(len(sys.argv) - 1, -1, -1):
    if sys.argv[s] == "--default-libmongoc":
        mongoc_src = sys.argv[s + 1]
        sys.argv.remove("--default-libmongoc")
        sys.argv.remove(mongoc_src)
    elif sys.argv[s] == "--default-libbson":
        bson_src = sys.argv[s + 1]
        sys.argv.remove("--default-libbson")
        sys.argv.remove(bson_src)

settings = {
    'export_symbols': ["monary_init",
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
                       "monary_insert"],
    'sources': [os.path.join("monary", "cmonary.c")],
    'include_dirs': [os.path.join(mongoc_src, "include", "libmongoc-1.0"),
                     os.path.join(bson_src, "include", "libbson-1.0")],
    'library_dirs': [os.path.join(mongoc_src, "lib"),
                     os.path.join(bson_src, "lib")],
    'libraries': libraries
}

test_requires = []
test_suite = "test"
if sys.version_info[:2] == (2, 6):
    test_requires.append("unittest2")
    test_suite = "unittest2.collector"

DEBUG = False

VERSION = "0.4.0"

CFLAGS = ["-fPIC", "-O2"]

if not DEBUG:
    CFLAGS.append("-DNDEBUG")


# Use pkg-config to find location of libbson and libmongoc.
try:
    import pkgconfig
except ImportError:
    # Set to default locations for libmongoc and libbson.
    warnings.warn(("WARNING: the python package pkgconfig is not installed. "
                   "If you have pkg-config installed on your system, please "
                   "install the python's pkgconfig, e.g. \"pip install "
                   "pkgconfig\". Will use libmongoc=%s and libbson=%s instead."
                   % (mongoc_src, bson_src)))

else:
    try:
        # Use pkgconfig to find location of libmongoc or libbson.
        if pkgconfig.exists("libmongoc-1.0"):
            pkgcfg = pkgconfig.parse("libmongoc-1.0")
            settings['include_dirs'] = list(pkgcfg['include_dirs'])
            settings['library_dirs'] = list(pkgcfg['library_dirs'])
            settings['libraries'] = list(pkgcfg['libraries'])
            settings['define_macros'] = list(pkgcfg['define_macros'])
        else:
            warnings.warn(("WARNING: unable to find libmongoc-1.0 with "
                           "pkgconfig. Please check that PKG_CONFIG_PATH is "
                           "set to a path that can find the .pc files for "
                           "libbson and libmongoc. Will use libmongoc=%s and "
                           "libbson=%s instead." % (mongoc_src, bson_src)))
    except EnvironmentError:
        warnings.warn(("WARNING: the system tool pkg-config is not installed. "
                      "Will use libmongoc=%s and libbson=%s instead."
                       % (mongoc_src, bson_src)))

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
    name="Monary",
    version=VERSION,
    packages=["monary"],
    install_requires=["pymongo", "numpy"],
    setup_requires=["pymongo", "numpy"],
    tests_require=test_requires,
    package_dir={"monary": "monary"},

    ext_modules=[module],

    author="David J. C. Beach",
    author_email="info@djcinnovations.com",
    description="Monary performs high-performance column queries on MongoDB",
    long_description=readme_content,
    keywords="monary pymongo mongo mongodb numpy array",
    classifiers=[
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
    url="http://bitbucket.org/djcbeach/monary/",
    test_suite=test_suite,
)
