#!/bin/bash

#java -jar target/simple-proxy-1.0-SNAPSHOT-jar-with-dependencies.jar 3080 9083 172.17.0.4 9083
java -jar target/simple-proxy-1.0-SNAPSHOT-jar-with-dependencies.jar 3080 \
  192.168.54.10:9083 \
  192.168.54.10:8020 \
  192.168.54.10:10000

