#! /usr/bin/env bash
RUNHOME=..

JAVA=$JAVA_HOME/bin/java
BINDIR=`pwd`


#CONF_DIR=./conf/
#CLASSPATH="${CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

for f in ${RUNHOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

for f in /usr/lib/sqoop2/client-lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done


#echo $CLASSPATH

java -cp $CLASSPATH io.transwarp.mysqlToHdfs $1
