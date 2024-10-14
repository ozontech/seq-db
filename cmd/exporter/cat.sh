#!/bin/bash

DIR=$1

(for f in `ls $DIR/docs_* | sort -r`; 
do gunzip -c $f
    echo
done) | grep -v -E '^$'