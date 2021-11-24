#! /bin/sh

# Make sure the script fails fast.
set -e
set -u
set -x

PROTO_DIR=groupcachepb
PROTO_CMD=/Users/zhangkun/study/protobuf/protoc/bin/protoc

$PROTO_CMD -I=$PROTO_DIR \
    --go_out=$PROTO_DIR \
    $PROTO_DIR/groupcache.proto

$PROTO_CMD -I=$PROTO_DIR \
   --go_out=. \
    $PROTO_DIR/example.proto
