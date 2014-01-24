package com.elex.bigdata.historytracer.mapreduce;

import com.elex.bigdata.historytracer.RowKeyUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 1/22/14
 * Time: 3:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class TableUidEventMapper extends TableMapper<Text,Text> {
  private static int dayLen=8,uidLen=5,seprateLen=1;
  private static Logger logger= Logger.getLogger(TableUidEventMapper.class);
  public void map(ImmutableBytesWritable row, Result value, Context context)
    throws IOException, InterruptedException {
      int rowLen=row.getLength(),eventLen=rowLen-dayLen-uidLen-seprateLen;
      Text uid= new Text(String.valueOf(Bytes.toLong(RowKeyUtils.extractUid(row.copyBytes()))));
      byte[] event=new byte[eventLen];
      System.arraycopy(row.copyBytes(),dayLen,event,0,eventLen);
      long timestap=value.raw()[0].getTimestamp();
      Text eventStr=new Text(String.valueOf(timestap)+":"+Bytes.toString(event));
//      logger.info("event :"+ eventStr.toString());
      context.write(uid,eventStr);
  }
}
