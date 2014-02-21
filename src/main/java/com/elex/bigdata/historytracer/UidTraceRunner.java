package com.elex.bigdata.historytracer;

import static com.elex.bigdata.historytracer.conf.HbaseNode.HBASE_NODES;

import com.elex.bigdata.historytracer.conf.HbaseNode;
import com.elex.bigdata.historytracer.model.UID;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Z J Wu Date: 14-1-14 Time: 下午6:12 Package: com.elex.bigdata.historytracer
 */
public class UidTraceRunner {
  private static final Logger LOGGER = Logger.getLogger(UidTraceRunner.class);

  private String table;
  private String event;
  private String date;
  private String uidFileOutput;

  public UidTraceRunner(String table, String event, String date, String uidFileOutput) {
    this.table = table;
    this.event = event;
    this.date = date;
    this.uidFileOutput = uidFileOutput;
  }

  public void run() throws InterruptedException {
    HBaseResourceManager manager;
    List<EventRowKeyUIDExtractor> extractors = new ArrayList<EventRowKeyUIDExtractor>(HBASE_NODES.length);
    EventRowKeyUIDExtractor extractor;
    CountDownLatch signal = new CountDownLatch(HBASE_NODES.length + 1);
    AtomicInteger producerCount = new AtomicInteger(HBASE_NODES.length);
    LinkedBlockingQueue<UID> uidBlockingXQueue = new LinkedBlockingQueue<UID>();
    for (HbaseNode hbaseNode : HBASE_NODES) {
      manager = new HBaseResourceManager(hbaseNode.getRootDir(),hbaseNode.getHost(), hbaseNode.getPort());
      extractor = new EventRowKeyUIDExtractor(manager, uidBlockingXQueue, signal, producerCount, hbaseNode.toString(),
                                              table, date, event);
      extractors.add(extractor);
    }

    for (EventRowKeyUIDExtractor ex : extractors) {
      new Thread(ex, "UidExtractThread@" + ex.getId()).start();
    }
    new Thread(new UIDWriter("Writer1", uidFileOutput, uidBlockingXQueue, signal, producerCount)).start();
    signal.await(1, TimeUnit.HOURS);
    LOGGER.info(
      "[UID-TRACE-RUNNER] - All uid dumped(Table=" + table + ", Event=" + event + ", date=" + date + ") to file " + uidFileOutput);
  }

  public static void main(String[] args) throws InterruptedException {
    if (ArrayUtils.isEmpty(args) || args.length < 4) {
      LOGGER.info("[RUNNER] - Must assign table, event, date and output uid file path.");
      System.exit(1);
    }
    String table = args[0];
    String date = args[1];
    String event = args[2];
    String output = args[3];

    UidTraceRunner runner = new UidTraceRunner(table, event, date, output);
    runner.run();
    File file = new File(output);
    String fileName = file.getName(), hdfsFile = "/user/hadoop/uid_trace/" + fileName;

//    try {
//      FileUtils.copy2HDFS(output, hdfsFile);
//    } catch (IOException e) {
//      LOGGER.error("[RUNNER] - Copy file failed.", e);
//      System.exit(1);
//    }
//    LOGGER.info("[RUNNER] - Local uid file(" + output + ") copied to HDFS(" + hdfsFile + ")");
  }
}
