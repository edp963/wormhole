#!/bin/bash

WORMHOLE_HOME=$(cd $(dirname $0); dirname "$PWD")

echo "[WormholeServer] Starts to shutdown system..."
#Server=`ps -ef | grep java | grep edp.rider.RiderStarter | grep -v "grep"| awk 'NR==1 {print $2}'`
Port=`grep port $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | sed 's/[[:space:]]//g'`
echo "[WormholeServer] Port: $Port"
Server=`lsof -i:$Port | grep '(LISTEN)' | awk '{print $2}'`
if [[ $Server -gt 0 ]]; then
  kill -9 $Server
  echo "[WormholeServer] System killed successfully, bye!!!"
else
  echo "[WormholeServer] System did not run."
fi
