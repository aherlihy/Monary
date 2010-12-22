gcc -c --std=c99 -O3 cmonary.c -I ~/Desktop/Mongo/mongodb-mongo-c-driver-7afb6e4/src/
gcc --std=c99 -dynamiclib -current_version 1.0 cmonary.o ~/Desktop/Mongo/mongodb-mongo-c-driver-7afb6e4/src/libmongo.a -o libcmonary.dylib
