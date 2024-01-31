#!/usr/bin/env bash
DEPLOY_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
JAVA_COMMAND=java
JAR_COMMAND=jar

if [[ -z "$JAVA_HOME" ]] ; then
  . /usr/lib/bigtop-utils/bigtop-detect-javahome
fi

if [[ -n "$JAVA_HOME" ]] ; then
  JAVA_COMMAND="$JAVA_HOME/bin/java"
  JAR_COMMAND="$JAVA_HOME/bin/jar"
fi

if [ ! -d $DEPLOY_SCRIPT_DIR/lib ] ; then
  mkdir "$DEPLOY_SCRIPT_DIR/lib"
  cd "$DEPLOY_SCRIPT_DIR/lib"
  $JAR_COMMAND xf "$DEPLOY_SCRIPT_DIR/agent.jar"
  cd "$DEPLOY_SCRIPT_DIR"
fi

if [[ -z "$JARS_DIR" ]] ; then
  AGENT_CLASSPATH="$(timeout 60 hadoop classpath)"
else
  AGENT_CLASSPATH="$JARS_DIR/*"
fi
#classes from agent go first
AGENT_CLASSPATH="$DEPLOY_SCRIPT_DIR/agent.jar:$DEPLOY_SCRIPT_DIR/lib/*:$AGENT_CLASSPATH"

HADOOP_CONF_DIR=${CONF_DIR:-'/etc/ozone/conf:/etc/hadoop/conf'}

export LOG_FILE=$DEPLOY_SCRIPT_DIR/logs/$$.log

nohup $JAVA_COMMAND -Djava.net.preferIPv4Stack=true -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=. -cp $HADOOP_CONF_DIR:$AGENT_CLASSPATH team.unison.remote.RemoteMain "$@" > $LOG_FILE 2>&1 &
