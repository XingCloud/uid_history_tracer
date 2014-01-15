package com.elex.bigdata.historytracer.model;

import com.xingcloud.basic.concurrent.BlockingXQueue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * User: Z J Wu Date: 14-1-14 Time: 下午6:23 Package: com.elex.bigdata.historytracer.model
 */
public class QueueMonitor<T> implements Runnable {

  private LinkedBlockingQueue<T> queue;

  private volatile boolean enabled;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public QueueMonitor(LinkedBlockingQueue<T> queue, boolean enabled) {
    this.queue = queue;
    this.enabled = enabled;
  }

  @Override public void run() {
    try {
      while (!Thread.interrupted() && enabled) {
        System.out.println(queue.size());
        TimeUnit.MILLISECONDS.sleep(500);
      }
    } catch (Exception e) {
      Thread.currentThread().interrupt();
    }
  }
}
