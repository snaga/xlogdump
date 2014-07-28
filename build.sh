#!/bin/sh

make clean-lib
rm test
make all-lib
make test
