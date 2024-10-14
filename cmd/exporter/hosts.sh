#!/bin/bash

(
    for i in `kubectl --context o-obs get po -o custom-columns='NAME:.metadata.name,IP:.status.podIP' | grep seqproxy-prod | awk '{print $2}'`
    do echo -ne ,$i
    done
) | tail -c +2