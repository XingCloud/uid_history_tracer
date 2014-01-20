package com.elex.bigdata.historytracer.mapreduce;

import com.elex.bigdata.historytracer.TracerConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-1-16 Time: 下午1:34 Package: com.elex.bigdata.historytracer.mapreduce
 */
public class LogLineDistinctMapper extends Mapper {
  @Override
  protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    context.write(new Text(line.trim()), TracerConstants.ONE);
  }
}
