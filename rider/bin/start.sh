#!/bin/bash
# sed wormholeServer host and port to index.html

WORMHOLE_HOME=$(cd $(dirname $0); dirname "$PWD")

# check if we have a valid WORMHOLE_HOME and if java is not available
if [ -z "${WORMHOLE_HOME}" ]; then
    echo "ERROR!!: WORMHOLE_HOME is not defined correctly, please specify WORMHOLE_HOME first."
    exit 1
fi

WORMHOLE_USER=`grep "wormholeServer.user" $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
WORMHOLE_KERBEROS_ENABLED=`grep "server.enabled" $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
WORMHOLE_KERBEROS_PARAM=` `
WORMHOLE_KERBEROS_SERVER=`grep "server.config" $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
WORMHOLE_KERBEROS_JAAS=`grep "jaas.startShell.config" $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
if [ -n "$WORMHOLE_KERBEROS_JAAS" ];then
  WORMHOLE_PRINCIPAL=`grep  "principal" $WORMHOLE_KERBEROS_JAAS | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
  WORMHOLE_KEYTAB=`grep  "keyTab" $WORMHOLE_KERBEROS_JAAS | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
fi


echo "WormholeServer user config in application.conf: $WORMHOLE_USER"

CURRENT_USER=`whoami`
echo "Current WormholeServer executing user: $CURRENT_USER"

if [ "$WORMHOLE_USER" != "$CURRENT_USER" ]; then
    echo "ERROR!!: Current WormholeServer executing user $CURRENT_USER doesn't match the spark.wormholeServer.user $WORMHOLE_USER in application.conf, please specify it first."
    exit 1
fi

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

if [[ $WORMHOLE_KERBEROS_ENABLED = true ]];then
   echo "try to verify via kdc server"
   kinit -kt $WORMHOLE_KEYTAB $WORMHOLE_PRINCIPAL
   WORMHOLE_KERBEROS_PARAM="-Djava.security.krb5.conf=$WORMHOLE_KERBEROS_SERVER -Djava.security.auth.login.config=$WORMHOLE_KERBEROS_JAAS"
fi

echo "wormholeServer final request address for UI: $finalAddress"

# sed -i "s#http://.*/api/v1#http://$finalAddress/api/v1#g" $WORMHOLE_HOME/webapp/index.html
# sed -i "s#http://.*/api-docs/swagger.json#http://$finalAddress/api-docs/swagger.json#g" $WORMHOLE_HOME/swagger-ui/index.html

echo "[WormholeServer] is starting..."
java -DWORMHOLE_HOME=$WORMHOLE_HOME $WORMHOLE_KERBEROS_PARAM -cp $WORMHOLE_HOME/lib/wormhole-rider-server_1.3-0.6.0.jar:$WORMHOLE_HOME/lib/* edp.rider.RiderStarter &
