from distutils.core import setup
from distutils.extension import Extension
from distutils.ccompiler import new_compiler

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
)

CMONARY_DIR = "cmonary/"
CMONGO_SRC = "mongodb-mongo-c-driver-7afb6e4/src/"
compiler = new_compiler()

CARGS = ["--std=c99", "-fPIC"]

def build_cmongo():
    import glob
    CMONGO_UNITS = glob.glob(CMONGO_SRC + "*.c")
    CMONGO_OBJECTS = [ f[:-2] + ".o" for f in CMONGO_UNITS ]
    compiler.compile(CMONGO_UNITS, extra_preargs=CARGS, include_dirs=[CMONGO_SRC])
    compiler.create_static_lib(CMONGO_OBJECTS, "mongo", CMONGO_SRC)

def build_cmonary():
    compiler.compile([CMONARY_DIR + "cmonary.c"],
                        extra_preargs=CARGS + ["-O3"],
                        include_dirs=[CMONGO_SRC])
    compiler.link_shared_lib([CMONARY_DIR + "cmonary.o", CMONGO_SRC + "libmongo.a"], "cmonary", output_dir="monary")

build_cmongo()
build_cmonary()
