package com.xingcloud.bigdata.observer.test;

import com.elex.bigdata.historytracer.UidTraceRunner;
import org.junit.Test;

/**
 * User: Z J Wu Date: 14-1-15 Time: 上午11:05 Package: com.xingcloud.bigdata.historytracer.test
 */
public class TestUIdRun {

  @Test
  public void test() throws InterruptedException {
    String table = "deu_age", date = "20140108", event = "visit", fileOut = "d:/uids/uid.log";
    UidTraceRunner runner = new UidTraceRunner(table, event, date, fileOut);
    runner.run();
  }
}
