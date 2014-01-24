package com.elex.bigdata.historytracer.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 1/22/14
 * Time: 3:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class UidJoinReducer extends Reducer<Text,Text,Text,Text> {
  private static Logger logger= Logger.getLogger(UidJoinReducer.class);
  @Override
  public void reduce(Text uid,Iterable<Text> eventOrTag,Context context) throws IOException, InterruptedException {
     boolean hasTag=false;
     StringBuilder builder=new StringBuilder();
     List<String> events= new ArrayList<String>();
     while(eventOrTag.iterator().hasNext()){
        String event=eventOrTag.iterator().next().toString();
        if(event.equals("Tag-Uid"))
        {
          hasTag=true;
//          logger.info("hasTag and uid is "+uid.toString());
        }
        else
          events.add(event);
     }
    String[] eventArr=events.toArray(new String[events.size()]);
    Arrays.sort(eventArr);
    for(String event :eventArr)
      builder.append(event+"\t");
    String totalEvents=builder.toString();
     if(hasTag && events.size()>0){
       logger.info("join success. uid is "+uid.toString()+" events "+totalEvents);
       context.write(uid,new Text(totalEvents));
     }else {
       logger.info("join failure. uid is "+uid.toString()+(hasTag?"Tag-Uid":" events "+totalEvents));
     }
  }
}
