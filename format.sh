#!/bin/bash

set -e

find . -name '*.hpp' | xargs clang-format -i
find . -name '*.cpp' | xargs clang-format -i