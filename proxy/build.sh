#!/bin/bash

cd `dirname $0`
rm -rf output/ > /dev/null 2>&1
mkdir -p output/bin

commit=$(git log -1| grep "^commit" | awk '{print $2}')
if [ "x$commit" != "x" ]
then
    echo "#define COMMIT \"$commit\"" > src/nc_commit.h
fi

make clean
make valgrind

cp src/nutcracker output/bin
echo "$commit" > output/commit
