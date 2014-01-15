package com.xingcloud.bigdata.observer.test;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

/**
 * User: Z J Wu Date: 14-1-6 Time: 下午6:21 Package: com.xingcloud.bigdata.historytracer.test
 */
public class TestStreamLogFilter {

  @Test
  public void test() throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));

    String date = "2014-01-13", projectId = "sof-apptools";
    String event = "stat.connect.normal.beginconnect";
    File file = new File("D:/securecrt_files/sof-apptools/" + projectId + ".stream." + date + ".log");
    File out = new File("D:/securecrt_files/sof-apptools/" + projectId + "." + event + ".uid." + date + ".log");
    BufferedReader br = new BufferedReader(new FileReader(file));
    PrintWriter pw = new PrintWriter(new FileWriter(out));
    Set<String> idSet = new HashSet<>(10000);
    String line;
    String[] arr;
    Set<String> events = new HashSet<>(10);
    while ((line = br.readLine()) != null) {
      arr = line.split("\t");
      if (projectId.equals(arr[0])) {
        if (event.equals("all")) {
          idSet.add(arr[1].trim());
          events.add(arr[2]);
        } else {
          if (arr[2].startsWith(event + ".")) {
            System.out.println(line);
            idSet.add(arr[1].trim());
            events.add(arr[2]);
          }
        }
      }
    }
    br.close();
    System.out.println(idSet.size());
    System.out.println(events);
    for (String uid : idSet) {
      pw.write(uid);
      pw.write("\n");
    }
    pw.close();
  }
}
