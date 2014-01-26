package com.xingcloud.bigdata.observer.test;

import com.elex.bigdata.historytracer.EventRowKeyUIDExtractor;
import com.elex.bigdata.historytracer.HBaseResourceManager;
import com.elex.bigdata.historytracer.UIDWriter;
import com.elex.bigdata.historytracer.model.QueueMonitor;
import com.elex.bigdata.historytracer.model.UID;
import com.xingcloud.basic.concurrent.BlockingXQueue;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Z J Wu Date: 14-1-14 Time: 下午2:27 Package: com.xingcloud.bigdata.historytracer.test
 */
public class TestUidExtract {

  public static class UIDPrinter implements Runnable {
    private BlockingXQueue<UID> queue;

    public UIDPrinter(BlockingXQueue<UID> queue) {
      this.queue = queue;
    }

    @Override
    public void run() {
      UID uid;
      while (!Thread.interrupted()) {
        try {
          uid = queue.take();
          if (UID.UID_POISON_PILL.equals(uid)) {
            break;
          }
          System.out.println(uid);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Test
  public void test() throws InterruptedException {
    HBaseResourceManager manager = new HBaseResourceManager("hdfs://namenode:19000/datanode1","datanode1", 3181);
    CountDownLatch signal = new CountDownLatch(2);
    String tableName = "deu_age", date = "20140108", event = "visit";
    LinkedBlockingQueue<UID> queue = new LinkedBlockingQueue<UID>();
    AtomicInteger producerCount = new AtomicInteger(1);
    EventRowKeyUIDExtractor uidExtractor = new EventRowKeyUIDExtractor(manager, queue, signal, producerCount, "001",
                                                                       tableName, date, event);
    Thread t = new Thread(uidExtractor, "uid-extract-thread");
    String filePath = "D:/uids/uid.log";
    t.start();
    t = new Thread(new UIDWriter("Writer1", filePath, queue, signal, producerCount), "uid-print-thread");
    t.start();
    t = new Thread(new QueueMonitor<UID>(queue, true));
    t.start();
    signal.await();
    System.out.println("All done.");
  }
}
