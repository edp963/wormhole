#!/bin/bash
# sed wormholeServer host and port to index.html

WORMHOLE_HOME=$(cd $(dirname $0); dirname "$PWD")

# check if we have a valid WORMHOLE_HOME and if java is not available
if [ -z "${WORMHOLE_HOME}" ]; then
    echo "ERROR!!: WORMHOLE_HOME is not defined correctly, please specify WORMHOLE_HOME first."
    exit 1
fi


HOST=`grep host $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
echo "wormholeServer host: $HOST"
PORT=`grep port $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | sed -e 's/[ \r\t]*//'`
echo "wormholeServer port: $PORT"

echo "[WormholeServer] is starting..."
java -Xmx4G -DWORMHOLE_HOME=$WORMHOLE_HOME -cp $WORMHOLE_HOME/lib/wormhole-rider-server_1.3-0.6.3.jar:$WORMHOLE_HOME/lib/* edp.rider.RiderStarter &
