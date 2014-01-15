package com.elex.bigdata.historytracer.conf;

import com.xingcloud.xa.conf.Config;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 14-1-14 Time: 下午4:16 Package: com.elex.bigdata.historytracer.conf
 */
public class HbaseNode {

  private static final Logger LOGGER = Logger.getLogger(HbaseNode.class);

  public static HbaseNode[] HBASE_NODES = initNodes();

  private int id;

  private String host;

  private int port;

  public int getId() {
    return id;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  private HbaseNode(int id, String host, int port) {
    this.id = id;
    this.host = host;
    this.port = port;
  }

  private static HbaseNode[] initNodes() {
    String multiHbaseConfFile = "/multi-hbases.properties";
    Configuration config = Config.createConfig(multiHbaseConfFile, Config.ConfigFormat.properties);
    String nodeString = config.getString("hbase.nodes");
    String[] nodeStringArray = nodeString.split("##");
    int a, b;
    char c1 = '#', c2 = ':';
    String idString, host, portString;
    HbaseNode[] nodes = new HbaseNode[nodeStringArray.length];
    for (int i = 0; i < nodeStringArray.length; i++) {
      a = nodeStringArray[i].indexOf(c1);
      b = nodeStringArray[i].indexOf(c2);
      idString = nodeStringArray[i].substring(0, a);
      host = nodeStringArray[i].substring(a + 1, b);
      portString = nodeStringArray[i].substring(b + 1);
      nodes[i] = new HbaseNode(Integer.valueOf(idString), host, Integer.valueOf(portString));
      LOGGER.info("[HBASE-NODE] - Node inited - " + nodes[i]);
    }
    return nodes;
  }

  @Override public String toString() {
    return "HbaseNode(" + id + "@" + host + ":" + port + ')';
  }
}
