#!/bin/bash

#source ${HOME}/.bash_profile

#The home directory for log-consumer.
export LOG_CONSUMER_HOME=${PWD%/consumer/*}

# The java implementation to use.  Required
export JAVA_HOME=/usr/java/default
export PATH=$JAVA_HOME/bin:$PATH
java -version
#The maximum amount of heap to use, in MB. Default is 2000.
export LOG_CONSUMER_HEAPSIZE=8192

#JVM parameter configuration
export OPT="-Xmx${LOG_CONSUMER_HEAPSIZE}m  -Xms${LOG_CONSUMER_HEAPSIZE}m  -Xmn3g -XX:PermSize=256M -XX:MaxPermSize=512m -Xss256k -XX:+UseParallelGC -XX:ParallelGCThreads=20 "

# Where log files are stored.  $HELLA_ENGINE_HOME/logs by default.
#export LOG_CONSUMER_LOG_DIR=$LOG_CONSUMER_HOME/logs
