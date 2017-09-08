#!/bin/bash
echo "[Rider3Server] is starting..."
java -Xms2g -Xmx4g -cp $RIDER_HOME/lib/wormhole-rider-server_1.2-0.3.0-SNAPSHOTS.jar:../lib/* edp.rider.RiderStarter &