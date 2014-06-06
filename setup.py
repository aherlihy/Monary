# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import glob
import platform
from distutils.core import setup, Command
from distutils.command.build import build
from distutils.ccompiler import new_compiler

DEBUG = False

VERSION = "0.2.3"

# Hijack the build process by inserting specialized commands into
# the list of build sub commands
build.sub_commands = [ ("build_cmongo", None), ("build_cmonary", None) ] + build.sub_commands

# Platform specific stuff
if platform.system() == 'Windows':
    compiler_kw = {'compiler' : 'mingw32'}
    linker_kw = {'libraries' : ['ws2_32']}
    so_target = 'cmonary.dll'
else:
    compiler_kw = {}
    linker_kw = {}
    so_target = 'libcmonary.so' 

compiler = new_compiler(**compiler_kw)

MONARY_DIR = "monary/"
CMONGO_SRC = "mongodb-mongo-c-driver-74cc0b8/src/"
CFLAGS = ["--std=c99", "-fPIC", "-O2"]

if not DEBUG:
    CFLAGS.append("-DNDEBUG")

# I suspect I could be using the build_clib command for this, but don't know how.
class BuildCMongoDriver(Command):
    """Custom command to build the C Mongo driver."""
    description = "builds the C Mongo driver"
    user_options = [ ]
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        CMONGO_UNITS = glob.glob(CMONGO_SRC + "*.c")
        CMONGO_OBJECTS = [ f[:-2] + ".o" for f in CMONGO_UNITS ]
        compiler.compile(CMONGO_UNITS, extra_preargs=CFLAGS, include_dirs=[CMONGO_SRC])
        compiler.create_static_lib(CMONGO_OBJECTS, "mongo", CMONGO_SRC)

class BuildCMonary(Command):
    """Custom command to build the cmonary library, static linking to the cmongo drivers,
       a producing a .so library that can be loaded via ctypes.
    """
    description = "builds the cmonary library (for ctypes)"
    user_options = [ ]
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        compiler.compile([MONARY_DIR + "cmonary.c"],
                         extra_preargs=CFLAGS,
                         include_dirs=[CMONGO_SRC])
        compiler.link_shared_lib([MONARY_DIR + "cmonary.o", CMONGO_SRC + "libmongo.a"],
                                 "cmonary", "monary", **linker_kw)

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
    requires = ["pymongo", "numpy"],
    
    package_dir = {"monary": "monary"},
    package_data = {"monary": [so_target]},

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
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database"
    ],
    url = "http://bitbucket.org/djcbeach/monary/",

    cmdclass = {
        'build_cmongo': BuildCMongoDriver,
        'build_cmonary': BuildCMonary,
    }
)
