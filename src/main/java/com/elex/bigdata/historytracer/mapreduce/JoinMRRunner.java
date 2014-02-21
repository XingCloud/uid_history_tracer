package com.elex.bigdata.historytracer.mapreduce;

import com.elex.bigdata.historytracer.conf.HbaseNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 1/22/14
 * Time: 3:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class JoinMRRunner implements Runnable {
  private static Logger logger = Logger.getLogger(JoinMRRunner.class);
  private static int dayRange = 3;
  private String tableName, day, hdfsIn, hdfsOut;
  private HbaseNode node;

  public JoinMRRunner(String tableName, String day, HbaseNode node, String hdfsIn, String hdfsOut) {
    this.tableName = tableName;
    this.day = day;
    this.node = node;
    this.hdfsIn = hdfsIn;
    this.hdfsOut = hdfsOut;
  }

  @Override
  public void run() {
    System.out.println("run...system");
    logger.info("run...");
    Configuration conf = new Configuration();
    conf.set("hbase.rootdir", node.getRootDir());
    conf.set("hbase.zookeeper.quorum", node.getHost());
    conf.setInt("hbase.zookeeper.property.clientPort", node.getPort());
    if(System.getProperty("dayRange")!=null)
       dayRange=Integer.parseInt(System.getProperty("dayRange"));
    try {
      Job job = Job.getInstance(conf);
      job.setJarByClass(JoinMRRunner.class);
      job.setJobName("JoinMRRunner");
      Scan scan = new Scan();
      scan.setCaching(5000);
      // get Start and End day
      DateFormat format=new SimpleDateFormat("yyyyMMdd");
      Date date=format.parse(day);
      Date start=new Date(date.getTime());
      start.setDate(date.getDate()-dayRange);
      Date end=new Date(date.getTime());
      end.setDate(date.getDate()+1);
      String startDay = format.format(start);
      String endDay = format.format(end);
      //set start stop Row
      scan.setStartRow(Bytes.toBytes(startDay));
      scan.setStopRow(Bytes.toBytes(endDay));
      logger.info("startDay: " + startDay + " endDay: " + endDay);
      TableMapReduceUtil.initTableMapperJob(tableName, scan, TableUidEventMapper.class, Text.class, Text.class, job);
      logger.info("init TableMapperJob");
      MultipleInputs.addInputPath(job, new Path(tableName), TableInputFormat.class, TableUidEventMapper.class);
      logger.info("add InputPath tableInputFormat");
      MultipleInputs.addInputPath(job, new Path(hdfsIn), TextInputFormat.class, FileUidMapper.class);
      logger.info("init inputpath textInputFormat");
      FileOutputFormat.setOutputPath(job, new Path(hdfsOut));
      job.setReducerClass(UidJoinReducer.class);
      try {
        job.waitForCompletion(true);
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      } catch (ClassNotFoundException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } finally {
    }
  }
}
