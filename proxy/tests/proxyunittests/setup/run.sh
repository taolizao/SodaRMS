#!/bin/bash
#author: taolizao@gmail.com
#updated: 2015-05-31

echo "make clean..."
make clean > make_clean.log 2>&1

echo "cp libs"
cp ../../../src/proto/libproto.a ../libs/
cp ../../../src/hashkit/libhashkit.a ../libs/

echo "make all..."
make all > make_all.log 2>&1

echo "run test_main"
cd ../bin/
#echo;echo;echo;echo;echo 
echo "=======================test result======================="
echo 
./test_main

