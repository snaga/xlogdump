#!/bin/sh

make clean-lib
rm test-xlogtranslate
make all-lib
make test-xlogtranslate
