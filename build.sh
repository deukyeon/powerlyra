#!/bin/bash -x

set -e

docker build -t powerlyra-build .
docker run --rm -v "$(pwd)":/powerlyra powerlyra-build /bin/bash -c "cd /powerlyra && ./configure && make -C /powerlyra/release/toolkits/graph_analytics -j4"