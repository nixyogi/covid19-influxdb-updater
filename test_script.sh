#!/bin/bash

# Orientation 
curl -i -XPOST 'http://148.251.91.243:8086/write?db=covid19_test' --data-binary \
'Thailand,state=NA latitude=15,longitude=101,confirmed=43 1583173800000000000'
