#!/bin/bash
# Copyright (c) 2021 Huawei Technologies Co., Ltd
#
# This software is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
# http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

set -e

BASE_DIR=$(cd "$(dirname "$0")"; pwd)
WORKER_DIR="${BASE_DIR}"/..
PROTO_DIR="${WORKER_DIR}"/../common/protobuf
PROTOBUF_VERSION=3.11.2
PROTOBUF_PROGECT_ADDRESS=https://github.com/protocolbuffers/protobuf.git
PKG_DIR=${BASE_DIR}/../../
GRPC_DIR=${PKG_DIR}/common/grpc/pb

cd "${PROTO_DIR}"

# Check if protoc is installed, intall it if not
if ! [ -x "$(command -v protoc)" ]; then
  PROTOBUF_DIR="${PROTO_DIR}"/protobuf
  if ! git clone -b "${PROTOBUF_VERSION}" "${PROTOBUF_PROGECT_ADDRESS}" protobuf; then
    echo "clone protoc project failed, please download and install it manually!!!"
    exit 1
  fi
  cd "${PROTOBUF_DIR}"
  if ! echo yes | apt-get install autoconf automake libtool curl make g++ zip unzip; then
    echo "apt install autoconf... tools failed!!!"
    exit 1
  fi
  if ! ./autogen.sh && ./configure && make -j 8 && make install; then
    echo "install protobuf tools failed"
    exit 1
  fi
  rm -rf "${PROTO_DIR}"/protobuf
fi
# Check if protoc-gen-go is installed, intall it if not
if ! [ -x "$(command -v protoc-gen-go)" ]; then
  if ! go get -u google.golang.org/protobuf/cmd/protoc-gen-go; then
    echo "go get protoc-gen-go failed"
    exit 1
  fi
fi

# Generate and copy golang protobuf file
if [ -d "${WORKER_DIR}"/runtime/rpc/pb ]; then
  rm -rf "${WORKER_DIR}"/runtime/rpc/pb
fi
if ! protoc --go_out=. rpc.proto; then
  echo "protoc rpc.proto to golang file failed!!!"
  exit 1
fi
if ! protoc --go_out=. stateful_event.proto; then
  echo "protoc stateful_event.proto to golang file failed!!!"
  exit 1
fi
mkdir -p "${WORKER_DIR}"/runtime/rpc/pb
if ! mv "${PROTO_DIR}"/*.go "${WORKER_DIR}"/runtime/rpc/pb/; then
  echo "failed to copy go protobuf source file!!!"
  exit 1
fi

if [ -d $GRPC_DIR ];then
    rm $GRPC_DIR -rf
fi
mkdir -p $GRPC_DIR
protoc --proto_path="${PKG_DIR}/common/protobuf" --go_out="$GRPC_DIR" --go-grpc_out="$GRPC_DIR" "${PKG_DIR}/common/protobuf/scheduler.proto"

echo "generate protobuf file successfully!"
exit 0
