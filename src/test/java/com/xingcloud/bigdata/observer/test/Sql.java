package com.xingcloud.bigdata.observer.test;

/**
 * User: Z J Wu Date: 14-1-13 Time: 上午11:47 Package: com.xingcloud.bigdata.historytracer.test
 */
public class Sql {
  public static void main(String[] args) {
    int dateInt = 20140101;
    String s1 = "mysql -hnode";
    String s2 = " -uxingyun -pOhth3cha -e\"SELECT t.uid AS uid FROM `16_fhw`.first_pay_time AS t WHERE t.val >= ";
    String s3 = "000000 AND t.val <= ";
    String s41 = "235959;\" >  ~/wuzijing/fhw.first_pay_time.uid.";
    String s42 = "235959;\" >>  ~/wuzijing/fhw.first_pay_time.uid.";

    String s;
    for (int i = 0; i < 12; i++) {
      s = s1 + 0 + s2 + (dateInt + i) + s3 + (dateInt + i) + s41 + (dateInt + i);
      System.out.println(s);
      for (int j = 1; j < 16; j++) {
        s = s1 + j + s2 + (dateInt + i) + s3 + (dateInt + i) + s42 + (dateInt + i);
        System.out.println(s);
      }
      System.out.println();
    }

  }
}
