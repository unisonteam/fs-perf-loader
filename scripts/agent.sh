#!/usr/bin/env bash
DEPLOY_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
JAVA_COMMAND=java
if [[ -n "$JAVA_HOME" ]] ; then
  JAVA_COMMAND="$JAVA_HOME/bin/java"
fi
 
AGENT_CLASSPATH="$(timeout 60 hadoop classpath)"
#classes from agent go first
AGENT_CLASSPATH="$DEPLOY_SCRIPT_DIR/agent.jar:$AGENT_CLASSPATH"
LOG_FILE=$DEPLOY_SCRIPT_DIR/logs/$$.log

nohup $JAVA_COMMAND -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=. -cp /etc/hadoop/conf:$JAVA_HOME/lib/tools.jar:$AGENT_CLASSPATH team.unison.remote.RemoteMain "$@" > $LOG_FILE 2>&1 &
