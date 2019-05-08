!/bin/bash
 
curdir=$(cd $(dirname $0); pwd)
 
execmd=$curdir/runtest
 
test_items="unit/printver unit/dump unit/auth unit/protocol unit/keyspace unit/scan unit/type/string unit/type/incr unit/type/list unit/type/list-2 unit/type/list-3 unit/type/set unit/type/zset unit/type/hash unit/sort unit/expire unit/other unit/multi unit/quit unit/aofrw integration/replication integration/replication-2 integration/replication-3 integration/replication-4 integration/replication-psync integration/aof integration/rdb integration/convert-zipmap-hash-on-load integration/logging unit/pubsub unit/slowlog unit/scripting unit/maxmemory unit/introspection unit/introspection-2 unit/limits unit/obuf-limits unit/bitops unit/bitfield unit/geo unit/memefficiency unit/hyperloglog"
 
function run_test_item()
{
    itemname="$1"
    echo 
    echo "================RUN TEST $itemname================="
    ./runtest --single $itemname
    echo "================RUN TEST $itemname DONE================="
    echo
}
 
for item in `echo "$test_items"`
do
    run_test_item "$item"
done
