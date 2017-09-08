#!/bin/bash
echo "[Rider3Server] Starts to shutdown system..."
Server=`ps -ef | grep java | grep edp.rider.RiderStarter | grep -v "grep"| awk 'NR==1 {print $2}'`
if [[ $Server -gt 0 ]]; then
  kill -9 $Server
  echo "[Rider3Server] System successed to be killed, bye!!!"
else
  echo "[Rider3Server] System did not run."
fi