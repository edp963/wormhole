#!/bin/bash

WORMHOLE_HOME=$(cd $(dirname $0); dirname "$PWD")

# check if we have a valid WORMHOLE_HOME/SPARK_HOME/HADOOP_HOME and if java is not available
if [ -z "${WORMHOLE_HOME}" ]; then
    echo "ERROR!!: WORMHOLE_HOME is not defined correctly, please specify WORMHOLE_HOME first."
    exit 1
fi


if [ -z "${SPARK_HOME}" ]; then
    echo "ERROR!!: SPARK_HOME is not defined correctly, please specify SPARK_HOME first."
    exit 1
fi

if [ -z "${HADOOP_HOME}" ]; then
    echo "ERROR!!: HADOOP_HOME is not defined correctly, please specify HADOOP_HOME first."
    exit 1
fi

echo "set hdfs permission for wormholeServer user"
#source /etc/profile
#source ~/.bash_profile

WORMHOLE_USER=`whoami`
echo "WormholeServer user: $WORMHOLE_USER"

WORMHOLE_HDFS_ROOT=`grep "wormhole.hdfs.root.path" $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
echo "WormholeServer hdfs root path: $WORMHOLE_HDFS_ROOT"

HOST=`grep host $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
echo "wormholeServer host: $HOST"

# get HDFS_SUPER_USERGROUP
if [ "$1" != "" ]
then
  TEMP=$1
  HDFS_SUPER_USERGROUP=${TEMP#*=}
else
  echo "didn't have --hdfs-user-supergroup arg, default set it as hadoop"
  HDFS_SUPER_USERGROUP=hadoop
fi
echo "HDFS_SUPER_USERGROUP: $HDFS_SUPER_USERGROUP"

echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -mkdir -p /user/$WORMHOLE_USER'"
su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -mkdir -p /user/$WORMHOLE_USER"

echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP /user/$WORMHOLE_USER'"
su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP /user/$WORMHOLE_USER"

echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -mkdir -p $WORMHOLE_HDFS_ROOT'"
su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -mkdir -p $WORMHOLE_HDFS_ROOT"

echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -mkdir -p $WORMHOLE_HDFS_ROOT/udfjars'"
su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -mkdir -p $WORMHOLE_HDFS_ROOT/udfjars"

echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP $WORMHOLE_HDFS_ROOT'"
su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP $WORMHOLE_HDFS_ROOT"

#SPARK_HOME=`grep "spark.home" $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
#echo "SPARK_HOME=$SPARK_HOME"

echo "WormholeServer auto set hdfs permission finish!!!"

SPARK_LOCAL_DIRS=`grep "SPARK_LOCAL_DIRS=" $SPARK_HOME/conf/spark-env.sh | tail -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`

if [ "$SPARK_LOCAL_DIRS" != "" ]
then
  echo "!!!Alert!!! spark local dirs is $SPARK_LOCAL_DIRS, please set WormholeServer user $WORMHOLE_USER has RW permission"
fi

echo "!!!Please test $WORMHOLE_USER ssh no password login by yourself!!!"
