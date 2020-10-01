#!/usr/bin/env bash

set -x
set -euo pipefail

rm -rf build && mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ../ && make VERBOSE=1
echo ""
cd ../ && time ./donors $*
