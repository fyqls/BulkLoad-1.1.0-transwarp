#! /bin/bash
USUAGE="Usuage: bash runBulkLoad.sh [config] <-test>"
PROJECT_HOME=../../
CLASSPATH=$PROJECT_HOME/bulkload.jar
IS_TEST=1  #Default is false


if [ "$#" -lt "1"  ]; then
  echo $USUAGE
  exit 1
elif [ "$#" -eq "1" ]; then
  CONF_FILE=$1
elif [ "$#" -eq "2" ]; then
  CONF_FILE=$1
  IS_TEST=0
else
  echo $USUAGE
  exit 1
fi

source $CONF_FILE

# When use the test mode, judge whether the test parameters have been set 
checktest=0
if [ "$IS_TEST" -eq "0" ]; then
  if [ "$outputDir" = "" ]; then
    echo "ERROR: You must set the outputDir in the conf file."
    checktest=1
  fi
  if [ "$tableName" = "" ]; then
    echo "ERROR: You must set the tableName in the conf file."
    checktest=1
  fi
fi

if [ "$checktest" -eq "1" ]; then
  exit 1
fi

HFILE_DIR=$outputDir
HBASE_TABLE_NAME=$tableName
INDEX_TABLE_NAME=$indextablename

echo "Start loading data into HBase at `date`"
start=`date +%s`


jars=`ls $PROJECT_HOME/lib`
for jar in $jars
do
    CLASSPATH="$CLASSPATH:$PROJECT_HOME/lib/$jar"
done
for f in /usr/lib/hadoop-mapreduce/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
for f in /usr/lib/hadoop-yarn/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

CLASSPATH=/etc/hadoop/conf:../..//conf:$CLASSPATH
echo "CLASSPATH:$CLASSPATH"
sudo -u hdfs hdfs dfs -rm -r $HFILE_DIR
if [ "$IS_TEST" -eq "0" ]; then
  sudo -u hdfs java -Djava.library.path=/usr/lib/hadoop/lib/native -cp $CLASSPATH com.transwarp.hbase.bulkload.ImportTextFile2HBase $CONF_FILE -test
else
  java -Djava.library.path=/usr/lib/hadoop/lib/native -cp $CLASSPATH com.transwarp.hbase.bulkload.ImportTextFile2HBase $CONF_FILE
fi

sudo -u hdfs hdfs dfs -chmod -R 777 $HFILE_DIR

if [ "$IS_TEST" -ne "0" ]
then
if [ "$INDEX_TABLE_NAME" != "" ]; then
  # Load the data into the index table from the index output dir
  /usr/lib/hbase/bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles $HFILE_DIR $INDEX_TABLE_NAME
else 
  /usr/lib/hbase/bin/hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles $HFILE_DIR $HBASE_TABLE_NAME
fi
fi

end=`date +%s`
interval=`expr $end - $start`
echo "End loading data into HBase at `date`"
echo "Time used $interval seconds"
