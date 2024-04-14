#!/bin/bash
   echo "loop 20 times to generate 560GB data duplicated"
   cp test_schema_mock.txt test_schema_mock.txt.BIG

for ((i=1;i<=20;i++));
do
   cp test_schema_mock.txt.BIG dup
   cat dup >> test_schema_mock.txt.BIG
done
   rm -rf dup
