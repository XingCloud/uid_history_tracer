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

  private String rootDir;

  private String host;

  private int port;

  public int getId() {
    return id;
  }

  public String getRootDir() {
    return rootDir;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  private HbaseNode(int id, String rootDir,String host, int port) {
    this.id = id;
    this.rootDir=rootDir;
    this.host = host;
    this.port = port;
  }

  private static HbaseNode[] initNodes() {
    String multiHbaseConfFile = "/multi-hbases.properties";
    Configuration config = Config.createConfig(multiHbaseConfFile, Config.ConfigFormat.properties);
    String nodeString = config.getString("hbase.nodes");
    String[] nodeStringArray = nodeString.split("##");
    String idString, rootDir,host, portString;
    HbaseNode[] nodes = new HbaseNode[nodeStringArray.length];
    for (int i = 0; i < nodeStringArray.length; i++) {
      String [] infos=nodeStringArray[i].split("#");
      idString = infos[0];
      rootDir=infos[1];
      String [] address=infos[2].split(":");
      host =address[0];
      portString = address[1];
      nodes[i] = new HbaseNode(Integer.valueOf(idString),rootDir, host, Integer.valueOf(portString));
      LOGGER.info("[HBASE-NODE] - Node inited - " + nodes[i]);
    }
    return nodes;
  }

  @Override public String toString() {
    return "HbaseNode(" + id + "#"+rootDir+"@" + host + ":" + port + ')';
  }


}
