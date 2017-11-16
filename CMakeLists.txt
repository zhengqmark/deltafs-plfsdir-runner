#
# CMakeLists.txt  cmake for deltafs plfsdir runner.
#

#
# specify the location of deltafs with CMAKE_INSTALL_PATH or
# CMAKE_PREFIX_PATH...
#
cmake_minimum_required(VERSION 3.1)

project (deltafs-plfsdir-runner C CXX)

set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release CACHE STRING
            "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
            "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()

set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package (Threads REQUIRED)
find_package (deltafs REQUIRED)

find_package (MPI MODULE REQUIRED)

add_executable (deltafs-plfsdir-runner deltafs-plfsdir-runner.cc)
target_link_libraries (deltafs-plfsdir-runner deltafs Threads::Threads)

# Note that the mpich on ub14 gives a leading space that we need to trim off.
string (REPLACE " " ";" mpicxx_flags "${MPI_CXX_COMPILE_FLAGS}")
foreach (lcv ${mpicxx_flags})
    if (NOT ${lcv} STREQUAL "")
        target_compile_options (deltafs-plfsdir-runner
                PUBLIC $<BUILD_INTERFACE:${lcv}>)
    endif ()
endforeach ()

foreach (lcv ${MPI_CXX_INCLUDE_PATH})
    target_include_directories (deltafs-plfsdir-runner
            PUBLIC $<BUILD_INTERFACE:${lcv}>)
endforeach ()

foreach (lcv ${MPI_CXX_LIBRARIES})
    target_link_libraries(deltafs-plfsdir-runner $<BUILD_INTERFACE:${lcv}>)
endforeach ()

set_property (TARGET deltafs-plfsdir-runner APPEND
        PROPERTY LINK_FLAGS ${MPI_CXX_LINK_FLAGS})

#
# "make install" rule
#
install (TARGETS deltafs-plfsdir-runner RUNTIME DESTINATION bin)
