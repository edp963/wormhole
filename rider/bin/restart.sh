#!/bin/bash

WORMHOLE_HOME=$(cd $(dirname $0); dirname "$PWD")

$WORMHOLE_HOME/bin/stop.sh
$WORMHOLE_HOME/bin/start.sh
