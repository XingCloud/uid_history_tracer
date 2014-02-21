package com.xingcloud.bigdata.observer.test;

import static com.elex.bigdata.historytracer.RowKeyUtils.COLUMN_VAL_BYTES;
import static com.elex.bigdata.historytracer.RowKeyUtils.FAMILY_VAL_BYTES;

import com.elex.bigdata.historytracer.HBaseResourceManager;
import com.elex.bigdata.historytracer.RowKeyUtils;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

/**
 * User: Z J Wu Date: 14-1-6 Time: 下午2:49 Package: com.xingcloud.bigdata.historytracer.test
 */
public class FakeDataGenerator {

  @Test
  public void test() throws IOException, ParseException {
    String date = "20140206";
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
    long timestamp = sdf.parse(date).getTime();
    String recordFilePath = "d:/misc/hbase_test/hbase.test.insert." + date + ".log";
    File f = new File(recordFilePath);
    PrintWriter pw = new PrintWriter(new FileWriter(f));
    String[] eventPool = new String[]{"visit.", "pay.", "heartbeat.", "pay.gross.", "pay.fee.", "CS.pri.smite.",
                                      "CS.pri.PowerWord.Shield", "CS.pri.PowerWord.Fortitude", "CS.pri.Chakra.Chastise",
                                      "CS.pri.Chakra.Sanctuary", "CS.pri.Chakra.Serenity", "CS.pri.ShadowWord.Pain",
                                      "CS.pri.LeapOfFaith", "CS.pri.HolyWord.Chastise", "CS.pri.GuardianSpirit"
    };
    eventPool = new String[]{"event_val_test."};

    final char tab = '\t';

    int eventCount = 30, batch = 1;
    Random random = new Random();
    int smallUid;
    byte hash;
    long bigUid;
    String event;
    byte[] rowkeyBytes;

    HBaseResourceManager manager = new HBaseResourceManager("hdfs://namenode:19000/datanode0","datanode0", 3181);
    HTablePool.PooledHTable pooledHTable = manager.getHTable("deu_age");
    List<Put> puts = new ArrayList<Put>(eventCount);
    Put put;
    long val;
    for (int j = 0; j < batch; j++) {
      for (int i = 0; i < eventCount; i++) {
        ++timestamp;
        smallUid = random.nextInt(5) + 1;
        hash = 2;
        bigUid = RowKeyUtils.hashUid2Long(hash, smallUid);
        event = eventPool[random.nextInt(eventPool.length)];
        rowkeyBytes = RowKeyUtils.buildRowKey(date, event, bigUid);
        val = (random.nextInt(1) + 1) * 10;
        put = new Put(rowkeyBytes);
        put.add(FAMILY_VAL_BYTES, COLUMN_VAL_BYTES, timestamp, Bytes.toBytes(val));
        puts.add(put);
        pw.write(date);
        pw.write(tab);
        pw.write(event);
        pw.write(tab);
        pw.write(String.valueOf(hash));
        pw.write(tab);
        pw.write(String.valueOf(smallUid));
        pw.write(tab);
        pw.write(String.valueOf(val));
        pw.write('\n');
      }
      pooledHTable.put(puts);
      puts.clear();
      pw.flush();
    }
    pw.close();

//    Scan scan = new Scan();
//    ResultScanner rs = pooledHTable.getScanner(scan);
//    System.out.println("---------------------------------");
//    List<Delete> deletes = new ArrayList<>();
//    for (Result r : rs) {
//      for (KeyValue kv : r.raw()) {
//        deletes.add(new Delete(kv.getRow()));
////        System.out.println(String
////                             .format("Rowkey=%s, Family=%s, Qualifier=%s, Value=%s", Bytes.toStringBinary(kv.getRow()),
////                                     Bytes.toString(kv.getFamily()), Bytes.toString(kv.getQualifier()),
////                                     Bytes.toStringBinary(kv.getValue())));
//      }
//    }
//    rs.close();
//    pooledHTable.delete(deletes);
    pooledHTable.close();
  }
}
