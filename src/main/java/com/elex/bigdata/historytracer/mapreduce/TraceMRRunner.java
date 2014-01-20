package com.elex.bigdata.historytracer.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-1-16 Time: 下午4:28 Package: com.elex.bigdata.historytracer.mapreduce
 */
public class TraceMRRunner {
  private static final Logger LOGGER = Logger.getLogger(TraceMRRunner.class);

  public static void main(String[] args) throws IOException {
    String hdfsIn = args[0], hdfsOut = args[1];
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(TraceMRRunner.class);
    job.setJobName("UidTraceJob");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setMapperClass(LogLineDistinctMapper.class);
    job.setReducerClass(LogLineDistinctReducer.class);
    FileInputFormat.setInputPaths(job, new Path(hdfsIn));
    FileOutputFormat.setOutputPath(job, new Path(hdfsOut));

    try {
      job.waitForCompletion(true);
    } catch (Exception e) {
      LOGGER.error(e);
    }
  }

}
