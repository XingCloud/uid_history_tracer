package com.elex.bigdata.historytracer;

import static com.elex.bigdata.historytracer.conf.HbaseNode.HBASE_NODES;

import com.elex.bigdata.historytracer.conf.HbaseNode;
import com.elex.bigdata.historytracer.model.UID;
import com.xingcloud.basic.concurrent.BlockingXQueue;
import org.apache.log4j.Logger;

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
    List<EventRowKeyUIDExtractor> extractors = new ArrayList<>(HBASE_NODES.length);
    EventRowKeyUIDExtractor extractor;
    CountDownLatch signal = new CountDownLatch(HBASE_NODES.length + 1);
    AtomicInteger producerCount = new AtomicInteger(HBASE_NODES.length);
    LinkedBlockingQueue<UID> uidBlockingXQueue = new LinkedBlockingQueue<>();
    for (HbaseNode hbaseNode : HBASE_NODES) {
      manager = new HBaseResourceManager(hbaseNode.getHost(), hbaseNode.getPort());
      extractor = new EventRowKeyUIDExtractor(manager, uidBlockingXQueue, signal, producerCount, hbaseNode.toString(),
                                              table, date, event);
      extractors.add(extractor);
    }

    Thread t;
    for (EventRowKeyUIDExtractor ex : extractors) {
      new Thread(ex, "UidExtractThread@" + ex.getId()).start();
    }
    new Thread(new UIDWriter("Writer1", uidFileOutput, uidBlockingXQueue, signal, producerCount)).start();
    signal.await(1, TimeUnit.HOURS);
    LOGGER.info(
      "[UID-TRACE-RUNNER] - All uid dumped(Table=" + table + ", Event=" + event + ", date=" + date + ") to file " + uidFileOutput);
  }

}
