#! /usr/bin/env bash

mvn clean package -DskipTests
cp -r target/oracle_lib build_1.0.0/
