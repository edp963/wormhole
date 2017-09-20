#!/bin/bash
echo "[WormholeServer] Starts to shutdown system..."
Server=`ps -ef | grep java | grep edp.rider.RiderStarter | grep -v "grep"| awk 'NR==1 {print $2}'`
if [[ $Server -gt 0 ]]; then
  kill -9 $Server
  echo "[WormholeServer] System killed successfully, bye!!!"
else
  echo "[WormholeServer] System did not run."
fi