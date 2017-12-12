#!/bin/bash
export MAD_HOME=/app/wormhole-test
echo "MAD_HOME" $MAD_HOME
java -DMAD_HOME=/app/wormhole-test -cp $MAD_HOME/lib/wormhole-mad-server_1.2-0.3.0-SNAPSHOTS.jar:$MAD_HOME/lib/* edp.mad.MadStarter &



