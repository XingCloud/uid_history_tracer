#!/bin/bash
 table=$1
 day=$2
 event=$3
 localOutPutFile=$4
 basedir=`dirname $0`/..
 #getUid
 rootDir="hdfs://namenode:19000"
 hdfsBaseDir="/user/hadoop/uid_trace"

 echo "java -cp ${basedir}/target/UIDHistoryTracer-jar-with-dependencies.jar com.elex.bigdata.historytracer.UidTraceRunner \
        ${table} ${day} ${event} ${localOutPutFile}"
 java -cp ${basedir}/target/UIDHistoryTracer-jar-with-dependencies.jar com.elex.bigdata.historytracer.UidTraceRunner \
 ${table} ${day} ${event} ${localOutPutFile}

 echo "hadoop fs -copyFromLocal ${localOutPutFile} ${hdfsBaseDir}/`basename ${localOutPutFile}`"
 hadoop fs -copyFromLocal ${localOutPutFile} ${hdfsBaseDir}/`basename ${localOutPutFile}`

 # get uniq Uids
 hdfsIn=${rootDir}${hdfsBaseDir}/`basename ${localOutPutFile}`
 hdfsOut=${rootDir}${hdfsBaseDir}/${table}${day}${event}

 echo "hadoop jar ${basedir}/target/UIDHistoryTracer-jar-with-dependencies.jar com.elex.bigdata.historytracer.mapreduce.TraceMRRunner \
        ${hdfsIn} ${hdfsOut}"
 hadoop jar ${basedir}/target/UIDHistoryTracer-jar-with-dependencies.jar com.elex.bigdata.historytracer.mapreduce.TraceMRRunner \
 ${hdfsIn} ${hdfsOut}

 #join uid with event.uid to get events sorted according to time

 joinHdfsIn=${hdfsOut}
 joinHdfsOut=${rootDir}${hdfsBaseDir}/${table}${day}${event}JoinResult

 echo "hadoop jar ${basedir}/target/UIDHistoryTracer-jar-with-dependencies.jar com.elex.bigdata.historytracer.UidJoinRunner \
        ${table} ${day} ${joinHdfsIn} ${joinHdfsOut}"
 hadoop jar ${basedir}/target/UIDHistoryTracer-jar-with-dependencies.jar com.elex.bigdata.historytracer.UidJoinRunner \
 ${table} ${day} ${joinHdfsIn} ${joinHdfsOut}

 # getMerge to Local
 LogBaseDir="/data/log/uid_trace_history"
 finalResultFile=${LogBaseDir}/${table}/${day}/${event}Result.log
 echo "hadoop fs -getmerge ${hdfsBaseDir}/${table}${day}${event}JoinResult/*/*  ${finalResultFile}"
 hadoop fs -getmerge ${hdfsBaseDir}/${table}${day}${event}JoinResult/*/*  ${finalResultFile}