#!/bin/bash

USG="Usage: $0  start|stop consumer-config log-dir [-debug]"
if [ $# -lt 3 ] ; then
  echo $USG
  exit 1
fi

source ./bin/consumer-env.sh

CONSUMER_CONFIG=$2
LOG_CONSUMER_LOG_DIR=$3

if [ ! -d $LOG_CONSUMER_LOG_DIR ] ; then
  mkdir -p $LOG_CONSUMER_LOG_DIR
fi

CLASSPATH=.:$JAVA_HOME/lib/tools.jar
LIBPATH=$LOG_CONSUMER_HOME/lib

cd $LOG_CONSUMER_HOME


CLASSPATH=$CLASSPATH:$LOG_CONSUMER_HOME/conf

for f in `find $LIBPATH -name '*.jar'`
  do
    CLASSPATH=$CLASSPATH:$f
  done
  

# ******************************************************************
# ** Set java runtime options                                     **
# ** Change 256m to higher values in case you run out of memory.  **
# ******************************************************************

OPT="$OPT -cp $CLASSPATH"
DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,address=10005,server=y,suspend=n"
if [ "$2" = "-debug" ] ; then
  OPT="$DEBUG $OPT"
fi 

# ***************
# ** Run...    **
# ***************

pid=$LOG_CONSUMER_LOG_DIR/asteroidea.pid
log=$LOG_CONSUMER_LOG_DIR/asteroidea.log

OPT=" $OPT -XX:+PrintClassHistogram -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC -Xloggc:$LOG_CONSUMER_LOG_DIR/gc_asteroidea.log"

if [ "$1" = "start" ] ; then
    mkdir -p ${LOG_CONSUMER_HOME}/webapps
    echo "start asteroidea , logging to $log"
    ENV="-Dconsumer.log.dir=$LOG_CONSUMER_LOG_DIR"
    exec java $ENV $OPT com.baofeng.dt.asteroidea.Asteroidea $CONSUMER_CONFIG 2>&1 < /dev/null &
    echo $! > $pid
elif [ "$1" = "stop" ] ; then
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo "stop asteroidea process ..."
        kill `cat $pid`
      else
        echo "no asteroidea to stop"
      fi
    else
      echo "no asteroidea to stop"
    fi
fi