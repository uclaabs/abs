#!/bin/bash

echo "[Main] sbt clean ..."
sbt/sbt clean

echo "[Main] merge hive libs"
mkdir -p lib_managed/jars
cp -r hive_lib/* lib_managed/jars

echo "[Main] done!"

