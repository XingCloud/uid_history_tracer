package com.elex.bigdata.historytracer;

import static com.elex.bigdata.historytracer.RowKeyUtils.MAX_BYTE;
import static com.elex.bigdata.historytracer.RowKeyUtils.extractUid;

import com.elex.bigdata.historytracer.model.UID;
import com.xingcloud.basic.concurrent.BlockingXQueue;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Z J Wu Date: 14-1-14 Time: 上午10:01 Package: com.elex.bigdata.historytracer
 */
public class EventRowKeyUIDExtractor implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(EventRowKeyUIDExtractor.class);

  private String id;

  private String tableName;

  private String singleDate;

  private String event;

  private Scan scan;

  private HBaseResourceManager manager;

  private LinkedBlockingQueue<UID> uidQueue;

  private CountDownLatch signal;

  private AtomicInteger producerCount;

  public String getId() {
    return id;
  }

  public EventRowKeyUIDExtractor(HBaseResourceManager manager, LinkedBlockingQueue<UID> queue, CountDownLatch signal,
                                 AtomicInteger producerCount, String id, String tableName, String singleDate,
                                 String event) {
    this.id = id;
    this.tableName = tableName;
    this.singleDate = singleDate;
    this.event = event;
    this.manager = manager;
    this.uidQueue = queue;
    this.signal = signal;
    this.producerCount = producerCount;
    this.scan = new Scan();
    byte[] startRowBytes = Bytes.toBytes(singleDate + event);
    byte[] stopRowBytes = new byte[startRowBytes.length + 2];
    System.arraycopy(startRowBytes, 0, stopRowBytes, 0, startRowBytes.length);
    stopRowBytes[stopRowBytes.length - 2] = MAX_BYTE;
    stopRowBytes[stopRowBytes.length - 1] = MAX_BYTE;
    scan.setStartRow(startRowBytes);
    scan.setStopRow(stopRowBytes);
    LOGGER.info("[UID-EXTRACTOR] - Extractor." + id + " inited(Table=" + tableName + ", RowkeyStart=" + Bytes
      .toStringBinary(startRowBytes) + ", RowkeyStop=" + Bytes.toStringBinary(stopRowBytes) + ").");
  }

  public String getSingleDate() {
    return singleDate;
  }

  public String getEvent() {
    return event;
  }

  @Override
  public void run() {
    HTablePool.PooledHTable pooledHTable = null;
    byte[] longByte;
    UID uid;
    long cnt = 0;
    boolean successful = false;
    try {
      pooledHTable = manager.getHTable(tableName);
      ResultScanner rs = pooledHTable.getScanner(scan);
      LOGGER.info("[UID-EXTRACTOR] - Begin scanning.");
      for (Result r : rs) {
        longByte = extractUid(r.getRow());
        if (longByte == null) {
          continue;
        }
        ++cnt;
        uid = new UID(longByte);
        uidQueue.put(uid);
      }
      successful = true;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      this.manager.closeTable(pooledHTable);
      this.producerCount.decrementAndGet();
      LOGGER.info(
        "[UID-EXTRACTOR] - Extractor." + id + " scanned " + cnt + " rows. Status=" + (successful ? "Success" : "Failed"
        ));
      signal.countDown();
    }
  }
}
