import glob

from distutils.core import setup, Command
from distutils.command.build import build
from distutils.ccompiler import new_compiler

MONARY_DIR = "monary/"
CMONGO_SRC = "mongodb-mongo-c-driver-7afb6e4/src/"
CFLAGS = ["--std=c99", "-fPIC", "-O2"]

build.sub_commands += [ ("build_cmongo", None), ("build_cmonary", None) ]

compiler = new_compiler()

class BuildCMongoDriver(Command):
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
        compiler.link_shared_lib([MONARY_DIR + "cmonary.o", CMONGO_SRC + "libmongo.a"], "cmonary")

setup(
    name = "Monary",
    version = "0.1",
    packages = ["monary"],
    package_dir = {"monary": "monary"},
    package_data = {"monary": ["libcmonary.so"]},

    author = "David J. C. Beach",
    author_email = "david@jcbeach.com",
    description = "Monary performs high-performance column queries from MongoDB.",
    keywords = "monary pymongo mongo mongodb numpy array",
    #url = ""

    cmdclass = {
        'build_cmongo': BuildCMongoDriver,
        'build_cmonary': BuildCMonary,
    }
)
