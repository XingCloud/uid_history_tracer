package com.elex.bigdata.historytracer;

import com.elex.bigdata.historytracer.conf.HbaseNode;
import com.elex.bigdata.historytracer.mapreduce.JoinMRRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 1/24/14
 * Time: 2:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class UidJoinRunner {
  private static Logger logger= Logger.getLogger(UidJoinRunner.class);
  public static void main(String[] args) throws InterruptedException, IOException {
    String localLogBase="/data/log/uid_trace_history";
    String tableName=args[0],day=args[1],hdfsIn=args[2],hdfsOut=args[3];
    List<JoinMRRunner> joinMRRunners=new ArrayList<JoinMRRunner>();
    CountDownLatch signal=new CountDownLatch(HbaseNode.HBASE_NODES.length+1);
    //ExecutorService executorService=new ThreadPoolExecutor(HbaseNode.HBASE_NODES.length,HbaseNode.HBASE_NODES.length,3600,TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>(HbaseNode.HBASE_NODES.length));
    ExecutorService executorService=Executors.newFixedThreadPool(HbaseNode.HBASE_NODES.length);
    for(int i=0;i<HbaseNode.HBASE_NODES.length;i++){
      HbaseNode node=HbaseNode.HBASE_NODES[i];
      JoinMRRunner joinMRRunner=new JoinMRRunner(tableName,day,node,hdfsIn,hdfsOut+"/"+node.getId());
      joinMRRunners.add(joinMRRunner);
    }
    for(JoinMRRunner runner :joinMRRunners){
      executorService.execute(runner);
    }
    logger.info("start " + HbaseNode.HBASE_NODES.length + "jobs");
    executorService.shutdown();
    executorService.awaitTermination(3600, TimeUnit.SECONDS);
    File localProjectDir=new File(localLogBase+"/"+tableName);
    if(!localProjectDir.exists())
      localProjectDir.mkdir();
    File localOutPutDir=new File(localLogBase+"/"+tableName+"/"+day);
    if(!localOutPutDir.exists())
      localOutPutDir.mkdir();

  }
}
