#!/bin/bash
echo "set hdfs permission for wormholeServer user"
#source /etc/profile
#source ~/.bash_profile

WORMHOLE_USER=`grep "wormholeServer.user" $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
echo "WormholeServer user: $WORMHOLE_USER"

WORMHOLE_HDFS_ROOT=`grep "wormhole.hdfs.root.path" $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
echo "WormholeServer hdfs root path: $WORMHOLE_HDFS_ROOT"

# get HDFS_SUPER_USERGROUP
if [ "$1" != "" ]
then
  TEMP=$1
  HDFS_SUPER_USERGROUP=${TEMP#*=}
else
  echo "didn't have --hdfs-user-supergroup arg, defalut set it as hadoop"
  HDFS_SUPER_USERGROUP=hadoop
fi
echo "HDFS_SUPER_USERGROUP=$HDFS_SUPER_USERGROUP"

echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -mkdir -p /user/$WORMHOLE_USER'"
su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -mkdir -p /user/$WORMHOLE_USER"

echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP /user/$WORMHOLE_USER'"
su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP /user/$WORMHOLE_USER"

echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -mkdir -p $WORMHOLE_HDFS_ROOT'"
su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -mkdir -p $WORMHOLE_HDFS_ROOT"

echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP $WORMHOLE_HDFS_ROOT'"
su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP $WORMHOLE_HDFS_ROOT"

SPARK_HOME=`grep "spark.home" $WORMHOLE_HOME/conf/application.conf | head -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
echo "SPARK_HOME=$SPARK_HOME"

SPARK_EVENTLOG=`grep "spark.eventLog.enabled" $SPARK_HOME/conf/spark-defaults.conf | tail -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`

if [[ $SPARK_EVENTLOG =~ .*true.* ]]
then
  SPARK_EVENTLOG_DIR=`grep "spark.eventLog.dir" $SPARK_HOME/conf/spark-defaults.conf | tail -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`
  echo "SPARK_EVENTLOG_DIR=$SPARK_EVENTLOG_DIR"
  if [[ x$SPARK_EVENTLOG_DIR =~ x.*directory.* ]]
  then
    SPARK_EVENTLOG_FINAL_DIR=/spark-history
    echo "SPARK_EVENTLOG_DIR=$SPARK_EVENTLOG_FINAL_DIR"
    echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -mkdir -p $SPARK_EVENTLOG_FINAL_DIR'"
    su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -mkdir -p $SPARK_EVENTLOG_FINAL_DIR"
    echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP $SPARK_EVENTLOG_FINAL_DIR'"
    su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP $SPARK_EVENTLOG_FINAL_DIR"
  else
    SPARK_EVENTLOG_FINAL_DIR=/${SPARK_EVENTLOG_DIR##*/}
    echo "SPARK_EVENTLOG_DIR=$SPARK_EVENTLOG_FINAL_DIR"
    echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -mkdir -p $SPARK_EVENTLOG_FINAL_DIR'"
    su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -mkdir -p $SPARK_EVENTLOG_FINAL_DIR"
    echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP $SPARK_EVENTLOG_FINAL_DIR'"
    su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP $SPARK_EVENTLOG_FINAL_DIR"
  fi
else
  if [[ ! $SPARK_EVENTLOG =~ .*false.* ]]
  then
    SPARK_EVENTLOG_FINAL_DIR=/spark-history
    echo "SPARK_EVENTLOG_DIR=$SPARK_EVENTLOG_FINAL_DIR"
    echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -mkdir -p $SPARK_EVENTLOG_FINAL_DIR'"
    su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -mkdir -p $SPARK_EVENTLOG_FINAL_DIR"
    echo "exec su - $HDFS_SUPER_USERGROUP -c 'hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP $SPARK_EVENTLOG_FINAL_DIR'"
    su - $HDFS_SUPER_USERGROUP -c "hdfs dfs -chown -R $WORMHOLE_USER:$HDFS_SUPER_USERGROUP $SPARK_EVENTLOG_FINAL_DIR"
  fi
fi

echo "!!!Alert!!!WormholeServer auto set hdfs permission finish, please check it"

SPARK_LOCAL_DIRS=`grep "SPARK_LOCAL_DIRS=" $SPARK_HOME/conf/spark-env.sh | tail -1 | cut -d = -f2 | cut -d \" -f2 | sed -e 's/[ \t\r]*//'`

if [ "$SPARK_LOCAL_DIRS" != "" ]
then
  echo "!!!Alert!!! spark local dirs is $SPARK_LOCAL_DIRS, please set WormholeServer user $WORMHOLE_USER has RW permission"
fi
