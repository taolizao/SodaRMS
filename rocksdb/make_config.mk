CC=cc
CXX=g++
PLATFORM=OS_LINUX
PLATFORM_LDFLAGS= -lpthread -lrt -lnuma
JAVA_LDFLAGS= -lpthread -lrt -lnuma
JAVA_STATIC_LDFLAGS= -lpthread -lrt
VALGRIND_VER=
PLATFORM_CCFLAGS= -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX  -DOS_LINUX -fno-builtin-memcmp -DROCKSDB_FALLOCATE_PRESENT -DNUMA -DROCKSDB_MALLOC_USABLE_SIZE -DROCKSDB_PTHREAD_ADAPTIVE_MUTEX -DROCKSDB_BACKTRACE -DROCKSDB_RANGESYNC_PRESENT -DROCKSDB_SCHED_GETCPU_PRESENT -march=native  -DROCKSDB_SUPPORT_THREAD_LOCAL
PLATFORM_CXXFLAGS=-std=c++11  -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX  -DOS_LINUX -fno-builtin-memcmp -DROCKSDB_FALLOCATE_PRESENT -DNUMA -DROCKSDB_MALLOC_USABLE_SIZE -DROCKSDB_PTHREAD_ADAPTIVE_MUTEX -DROCKSDB_BACKTRACE -DROCKSDB_RANGESYNC_PRESENT -DROCKSDB_SCHED_GETCPU_PRESENT -march=native  -DROCKSDB_SUPPORT_THREAD_LOCAL
PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_EXT=so
PLATFORM_SHARED_LDFLAGS=-Wl,--no-as-needed -shared -Wl,-soname -Wl,
PLATFORM_SHARED_VERSIONED=true
EXEC_LDFLAGS=
JEMALLOC_INCLUDE=
JEMALLOC_LIB=
ROCKSDB_MAJOR=5
ROCKSDB_MINOR=7
ROCKSDB_PATCH=0
CLANG_SCAN_BUILD=scan-build
CLANG_ANALYZER=
PROFILING_FLAGS=-pg
LUA_PATH=
