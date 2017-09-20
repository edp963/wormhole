#!/bin/bash
# sed wormholeServer host and port to index.html
# domain.url
DORMAIN=`grep "domain.url" $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
echo "wormholeServer domainUrl: $DORMAIN"
HOST=`grep host $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
echo "wormholeServer host: $HOST"
PORT=`grep port $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | sed -e 's/[ \r\t]*//'`
echo "wormholeServer port: $PORT"
if [[ $DORMAIN = "" ]]
then finalAddress=$HOST:$PORT
else finalAddress=${DORMAIN:7}
fi
echo "wormholeServer final request address for UI: $finalAddress"
sed -i "s#http://.*/api/v1#http://$finalAddress/api/v1#g" $WORMHOLE_HOME/webapp/index.html
sed -i "s#http://.*/api-docs/swagger.json#http://$finalAddress/api-docs/swagger.json#g" $WORMHOLE_HOME/swagger-ui/index.html

echo "[WormholeServer] is starting..."
java -DWORMHOLE_HOME=$WORMHOLE_HOME -cp $WORMHOLE_HOME/lib/wormhole-rider-server_1.2-0.3.0-SNAPSHOTS.jar:$WORMHOLE_HOME/lib/* edp.rider.RiderStarter &