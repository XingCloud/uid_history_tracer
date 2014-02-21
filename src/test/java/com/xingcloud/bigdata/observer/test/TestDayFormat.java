package com.xingcloud.bigdata.observer.test;

import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 2/10/14
 * Time: 4:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestDayFormat {
  @Test
  public void test() throws ParseException {
    String day="20140201";
    int dayRange=3;
    DateFormat format=new SimpleDateFormat("yyyyMMdd");
    Date date=format.parse(day);
    Date start=new Date(date.getTime());
    start.setDate(date.getDate()-dayRange);
    Date end=new Date(date.getTime());
    end.setDate(date.getDate()+1);
    String startDay = format.format(start);
    String endDay = format.format(end);
  }
}
