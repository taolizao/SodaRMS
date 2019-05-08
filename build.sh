#!/bin/bash

curr_path=$(cd $(dirname $0); pwd)

cd ${curr_path}/rocksdb
make static_lib

cd ${curr_path}/public_lib
make clean all

cd ${curr_path}

commit=$(git log -1| grep "^commit" | awk '{print $2}')
commitversion="V_$commit"
echo "#define COMMITVER \"$commitversion\"" > src/git_version.h

make clean all

cd ${curr_path}
mkdir -p output
cp src/redis-server src/redis-cli src/redis-check-aof src/redis-benchmark src/redis-check-rdb  src/redis-sentinel output/

cd ${curr_path}/proxy
sh build.sh
cp ${curr_path}/proxy/src/nutcracker ${curr_path}/output/

echo "$commitversion" > output/commit

