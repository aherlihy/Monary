cmake_minimum_required(VERSION 2.8)

project(monary)

# Add in our modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

# Set PKG_CONFIG_PATH
if(DEFINED $ENV{PKG_CONFIG_PATH})
    set(PKG_CONFIG_PATH $ENV{PKG_CONFIG_PATH})
else()
    set(PKG_CONFIG_PATH "${CMAKE_INSTALL_PREFIX}/lib/pkgconfig")
endif()

find_package(bson REQUIRED)
find_package(mongoc REQUIRED)

add_library(cmonary SHARED cmonary.c)

include_directories(/opt/include/libbson-1.0)
include_directories(/opt/include/libmongoc-1.0)

install(TARGETS cmonary DESTINATION .)
target_link_libraries(cmonary ${BSON_LIBRARIES})
target_link_libraries(cmonary ${MONGOC_LIBRARIES})
