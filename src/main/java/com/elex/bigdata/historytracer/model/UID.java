package com.elex.bigdata.historytracer.model;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * User: Z J Wu Date: 14-1-14 Time: 下午2:21 Package: com.elex.bigdata.historytracer
 */
public class UID {

  public static final UID UID_POISON_PILL = new UID();

  private byte[] uidBytes;

  private UID() {
  }

  public UID(byte[] uidBytes) {
    this.uidBytes = uidBytes;
  }

  public byte[] getUidBytes() {
    return uidBytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UID)) {
      return false;
    }

    UID uid = (UID) o;

    if (!Arrays.equals(uidBytes, uid.uidBytes)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return uidBytes != null ? Arrays.hashCode(uidBytes) : 0;
  }

  @Override
  public String toString() {
    return String.valueOf(Bytes.toLong(uidBytes));
  }
}
