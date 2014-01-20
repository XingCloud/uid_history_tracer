package com.elex.bigdata.historytracer.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-1-16 Time: 下午4:14 Package: com.elex.bigdata.historytracer.mapreduce
 */
public class LogLineDistinctReducer extends Reducer {

  @Override protected void reduce(Object key, Iterable values, Context context) throws IOException,
    InterruptedException {
    context.write(new Text(key.toString()), NullWritable.get());
  }
}
