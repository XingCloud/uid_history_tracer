package com.elex.bigdata.historytracer;

/**
 * User: Z J Wu Date: 14-1-15 Time: 上午11:30 Package: com.elex.bigdata.historytracer
 */
public class PrudocerCount {
  private int count;

  public PrudocerCount(int count) {
    this.count = count;
  }

  public synchronized void decrease() {
    --this.count;
  }

  public synchronized int get() {
    return this.count;
  }
}
