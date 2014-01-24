package com.elex.bigdata.historytracer.mapreduce;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 1/22/14
 * Time: 3:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileUidMapper extends Mapper<Object,Text,Text,Text> {
  private static Logger logger= Logger.getLogger(FileUidMapper.class);
  public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
    Text uid=new Text(value.toString());
    String CollectionTag=new String("Tag-Uid");
//    logger.info(uid.toString()+": "+CollectionTag);
    context.write(uid,new Text(CollectionTag));
  }
}
