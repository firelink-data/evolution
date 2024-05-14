#!/bin/bash

TEST_FILE=$1
echo "Loop $2 times to duplicate data to $TEST_FILE"

for ((i=1;i<=$2;i++));
do
   cat "./$TEST_FILE" >> "BIG-$TEST_FILE"
done

