PROJECT_HOME=../../
CLASSPATH=$PROJECT_HOME/bulkload.jar
#RECORD_COUNT_FILE=$1
#RECORDS_PER_REGION=$2

if [ $# -ne 2 ]
then
  echo "usage : genSplitKey filePath sampleNum"
  exit -1
fi

RECORD_COUNT_FILE=$1
RECORDS_PER_REGION=$2


#create external table in hive, generate the record_count file
echo "Start generating splitKey at `date`"
start=`date +%s`

#hive -f gen_record_count.hql


# generate the splitKeySpec String
jars=`ls $PROJECT_HOME/lib`
for jar in $jars
do
    CLASSPATH="$CLASSPATH:$PROJECT_HOME/lib/$jar"
done

CLASSPATH=/usr/lib/hadoop/conf:/usr/lib/hbase/conf:$CLASSPATH

java -cp $CLASSPATH com.transwarp.hbase.bulkload.common.RegionSplitKeyUtils $RECORD_COUNT_FILE $RECORDS_PER_REGION


end=`date +%s`
interval=`expr $end - $start`
echo "End generating splitKey at `date`"
echo "Time used $interval seconds"


