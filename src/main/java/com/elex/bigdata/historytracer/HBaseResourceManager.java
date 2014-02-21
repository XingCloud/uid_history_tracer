package com.elex.bigdata.historytracer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.Closeable;
import java.io.IOException;

/**
 * User: Z J Wu Date: 14-1-6 Time: 下午2:20 Package: com.elex.bigdata.historytracer
 */
public class HBaseResourceManager implements Closeable {

  private Configuration conf;
  private HTablePool pool;
  private final int MAX_POOL_SIZE = 200;
  private final String EVENT_TABLE_SUFFIX = "_deu";

  public HBaseResourceManager(String rootDir,String host, int port) {
    this.conf = HBaseConfiguration.create();
    conf.set("hbase.rootdir",rootDir);
    conf.set("hbase.zookeeper.quorum", host);
    conf.setInt("hbase.zookeeper.property.clientPort", port);
    this.pool = new HTablePool(this.conf, this.MAX_POOL_SIZE);
  }

  public HTablePool.PooledHTable getHTable(String tableName) throws IOException {
    try {
      return (HTablePool.PooledHTable) pool.getTable(tableName);
    } catch (Exception e) {
      throw new IOException("Cannot get htable from hbase(" + tableName + ").", e);
    }
  }

  public void closeTable(HTablePool.PooledHTable hTable) {
    if (hTable != null) {
      try {
        hTable.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void closeAll() throws IOException {
    this.pool.close();
  }

  public void closeAll(String projectId) throws IOException {
    this.pool.closeTablePool(projectId + EVENT_TABLE_SUFFIX);
  }

  @Override
  public void close() throws IOException {
    HConnectionManager.deleteAllConnections(true);
  }

}
