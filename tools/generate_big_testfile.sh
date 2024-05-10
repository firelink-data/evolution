#!/bin/bash

TEST_FILE="test_schema_mock.flf"
echo "Loop $1 times to duplicate data to $TEST_FILE"

for ((i=1;i<=$1;i++));
do
   cat "./resources/test-flf/$TEST_FILE" >> "BIG-$TEST_FILE"
done

