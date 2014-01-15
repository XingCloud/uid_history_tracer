package com.xingcloud.bigdata.observer.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-1-6 Time: 下午4:38 Package: com.xingcloud.bigdata.historytracer.test
 */
public class TestHadoopClient {

  @Test
  public void test() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/home/hadoop/"), false);
    System.out.println(iterator);
  }
}
