package com.elex.bigdata.historytracer;

import com.elex.bigdata.historytracer.model.UID;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: Z J Wu Date: 14-1-14 Time: 下午2:50 Package: com.elex.bigdata.historytracer
 */
public class UIDWriter implements Runnable {

  private static final Logger LOGGER = Logger.getLogger(UIDWriter.class);

  private String id;

  private String outputFilePath;

  private CountDownLatch signal;

  private LinkedBlockingQueue<UID> queue;

  private final AtomicInteger producerCount;

  public UIDWriter(String id, String outputFilePath, LinkedBlockingQueue<UID> queue, CountDownLatch signal,
                   AtomicInteger producerCount) {
    this.id = id;
    this.outputFilePath = outputFilePath;
    this.queue = queue;
    this.signal = signal;
    this.producerCount = producerCount;
    LOGGER.info("[UID-WRITER] - Writer(" + id + ") begin to write to file " + outputFilePath);
  }

  @Override
  public void run() {
    UID uid;
    PrintWriter pw = null;
    long cnt = 0;
    try {
      pw = new PrintWriter(new FileWriter(new File(outputFilePath)));
      while (!Thread.interrupted() && producerCount.get() > 0) {
        uid = queue.poll();
        if (uid == null) {
          continue;
        }
        pw.write(uid.toString());
        pw.write('\n');
        ++cnt;
      }
      LOGGER.info("[UID-WRITER] - All producer stopped offering uid.");
    } catch (Exception e) {
      Thread.currentThread().interrupt();
    } finally {
      LOGGER.info("[UID-WRITER] - Writer(" + id + ") finished writing(" + cnt + " lines) to file " + outputFilePath);
      IOUtils.closeQuietly(pw);
      signal.countDown();
    }
  }
}
