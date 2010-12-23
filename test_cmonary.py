from monary import Monary
import numpy

monary = Monary("127.0.0.1")
arrays =  monary.query("test", "spam", {"float5": {"$gt": 6.8}}, ["_id", "intval", "float1", "float2", "float3", "float4", "float5", "float6", "float7", "float8", "float9", "float10"], ["id", "int32", "float64", "float64", "float64", "float64", "float64", "float64", "float64", "float64", "float64", "float64"], limit=5000000, do_count=False)
monary.close()

#import struct
# (b1, b2, b3) = struct.unpack(">III", arrays[0][0])
# val = (b1 << 64) + (b2 << 32) + b3
# print "%x" % val

for array in arrays[1:]:
    #print array[0:10]
    print numpy.mean(array)


