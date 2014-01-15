package com.elex.bigdata.historytracer;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * User: Z J Wu Date: 14-1-13 Time: 下午2:39 Package: com.elex.bigdata.historytracer
 */
public class RowKeyUtils {

  public static final byte MAX_BYTE = -1;

  public static final byte[] FAMILY_VAL_BYTES = Bytes.toBytes("val");
  public static final byte[] COLUMN_VAL_BYTES = FAMILY_VAL_BYTES;

  public static byte[] extractUid(byte[] rowkeyBytes) {
    if (ArrayUtils.isEmpty(rowkeyBytes)) {
      return null;
    }
    int index = -1;
    for (int i = 0; i < rowkeyBytes.length; i++) {
      if (MAX_BYTE == rowkeyBytes[i]) {
        index = i;
      }
    }
    if (index == -1) {
      return null;
    }
    byte[] longBytes = new byte[Bytes.SIZEOF_LONG];
    System.arraycopy(rowkeyBytes, index + 1, longBytes, 3, rowkeyBytes.length - index - 1);
    return longBytes;
  }

  public static long hashUid2Long(byte hash, int uid) {
    byte[] bytes = new byte[8];
    bytes[3] = hash;
    bytes[4] = (byte) (uid >> 24 & 0xff);
    bytes[5] = (byte) (uid >> 16 & 0xff);
    bytes[6] = (byte) (uid >> 8 & 0xff);
    bytes[7] = (byte) (uid & 0xff);
    return Bytes.toLong(bytes);
  }

  public static byte[] buildRowKey(String date, String event, long uid) {
    byte[] bytesTmp = Bytes.toBytes(date + event);
    int lenTmp = bytesTmp.length;

    byte[] rk = new byte[lenTmp + 6];
    System.arraycopy(bytesTmp, 0, rk, 0, lenTmp);

    rk[lenTmp] = MAX_BYTE;
    ++lenTmp;

    rk[lenTmp] = (byte) (uid >> 32 & 0xff);
    rk[lenTmp + 1] = (byte) (uid >> 24 & 0xff);
    rk[lenTmp + 2] = (byte) (uid >> 16 & 0xff);
    rk[lenTmp + 3] = (byte) (uid >> 8 & 0xff);
    rk[lenTmp + 4] = (byte) (uid & 0xff);
    return rk;
  }

  public static void main(String[] args) {
    byte[] bytes = new byte[]{1, 2, 3, 4, 5, 6, -1, 0, 3, 0, 1, 2};
    byte[] longBytes = extractUid(bytes);
    System.out.println(Bytes.toLong(longBytes));
  }

}
