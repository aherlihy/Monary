# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

import os
import platform
import re
import subprocess
import sys
from distutils.core import Command
from distutils.command.build import build
from distutils.ccompiler import new_compiler

# Don't force people to install setuptools unless
# we have to.
try:
    from setuptools import setup
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup

DEBUG = False

VERSION = "0.3.0"

# Hijack the build process by inserting specialized commands into
# the list of build sub commands
build.sub_commands = [ ("build_cmongo", None), ("build_cmonary", None) ] + build.sub_commands

# Platform specific stuff
if platform.system() == 'Windows':
    compiler_kw = {'compiler' : 'mingw32'}
    linker_kw = {'libraries' : ['ws2_32']}
    so_target = 'libcmonary.dll'
else:
    compiler_kw = {}
    linker_kw = {'libraries' : []}
    so_target = 'libcmonary.so' 

compiler = new_compiler(**compiler_kw)

MONARY_DIR = "monary"
CMONGO_SRC = "mongodb-mongo-c-driver-0.98.0"
CFLAGS = ["-fPIC", "-O2"]

if not DEBUG:
    CFLAGS.append("-DNDEBUG")

class BuildException(Exception):
    """Indicates an error occurred while compiling from source."""
    pass

# I suspect I could be using the build_clib command for this, but don't know how.
class BuildCMongoDriver(Command):
    """Custom command to build the C Mongo driver. Relies on autotools."""
    description = "builds the C Mongo driver"
    user_options = [ ]
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        try:
            os.chdir(CMONGO_SRC)
            status = subprocess.call(["./configure", "--enable-static",
                                      "--without-documentation",
                                      "--disable-maintainer-mode",
                                      "--disable-tests", "--disable-examples"])
            if status != 0:
                raise BuildException("configure script failed with exit status %d" % status)

            status = subprocess.call(["make"])
            if status != 0:
                raise BuildException("make failed with exit status %d" % status)
            # after configuring, add libs to linker_kw
            with open(os.path.join("src", "libmongoc-1.0.pc")) as f:
                libs = re.search(r"Libs:\s+(.+)$", f.read(), flags=re.MULTILINE).group(1)
                libs = [l[2:] for l in re.split(r"\s+", libs)[:-1] if l.startswith("-l")]
                linker_kw["libraries"] += libs
        finally:
            os.chdir("..")

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
        compiler.compile([os.path.join(MONARY_DIR, "cmonary.c")],
                         extra_preargs=CFLAGS,
                         include_dirs=[os.path.join(CMONGO_SRC, "src", "mongoc"),
                                       os.path.join(CMONGO_SRC, "src", "libbson", "src", "bson")])
        compiler.link_shared_lib([os.path.join(MONARY_DIR, "cmonary.o"),
                                  os.path.join(CMONGO_SRC, ".libs", "libmongoc-1.0.a"),
                                  os.path.join(CMONGO_SRC, "src", "libbson", ".libs", "libbson-1.0.a")],
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
