package com.elex.bigdata.historytracer;

import com.xingcloud.uidtransform.HbaseMysqlUIDTruncator;
import com.xingcloud.uidtransform.StreamLogUidTransformer;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 2/10/14
 * Time: 5:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class TransferUid {
  private static Logger logger=  Logger.getLogger(TransferUid.class);
  public static void main(String[] args) throws Exception {
     long start=System.currentTimeMillis();
     // get projectId
     String projectId=args[1];
     // get uid File
     String  uidFilePath=args[0];
     File    uidFile=new File(uidFilePath);
     logger.info("uidFile path "+uidFile.getPath());
     // create origUidFile
     String name =uidFile.getName();
     File origUidFile=new File(uidFile.getParent()+"/"+"orig"+name);
     logger.info("origUidFile path "+origUidFile.getPath());

     int batchSize=50000;
    /*read uidFile and batch(1000 lines) process
       get uid from each line(first field in line terminated with '\t' and add to List<Uid> uids;
       get events connected to uid(other fields in line) and add to List<String> events;
       if uids'size ==1000 then use uid-transformer to transfrom them and reset list;
          trunctedUids=HbaseMysqlUIDTruncator.truncate(List<String> uids)
          List<String> origUids=StreamLogUidTransformer.transform(projectId,truncatedUids,fasle(debug));
          write origUids and events to origUidFile
          reset uids origUids and events;
     */
     BufferedReader reader=new BufferedReader(new FileReader(uidFile));
     BufferedWriter writer=new BufferedWriter(new FileWriter(origUidFile));
     String line;
     List<String> uids=new ArrayList<String>(),events=new ArrayList<String>();
     logger.info("transfer start");
     try{
        while((line=reader.readLine())!=null){
            int index=line.indexOf('\t');
            if(index<0)continue;
            String uid=line.substring(0,index);
            String event=line.substring(index+1);
            uids.add(uid);
            events.add(event);
            if(uids.size()==batchSize){
              List<String> origUids=transform(uids,events,projectId);
              for(int i=0;i<batchSize;i++){
                writer.write(origUids.get(i)+"\t"+events.get(i));
                writer.newLine();
              }
            }
        }
        List<String> origUids=transform(uids,events,projectId);
        for(int i=0;i<uids.size()-1;i++){
          writer.write(origUids.get(i)+"\t"+events.get(i));
          writer.newLine();
        }
        writer.write(origUids.get(origUids.size()-1)+"\t"+events.get(events.size()-1));
        logger.info("transfer completed");
        logger.info("transfer using "+(System.currentTimeMillis()-start)+" ms");
     }catch (IOException e){
        e.printStackTrace();
     }finally {
       reader.close();
       writer.flush();
       writer.close();
     }
  }

  public static  List<String>  transform(List<String> uids,List<String> events,String projectId) throws Exception {
    List<Long> truncatedUids= HbaseMysqlUIDTruncator.truncate(uids);
    List<String> origUids=StreamLogUidTransformer.INSTANCE.transform(projectId,truncatedUids,false);
    return origUids;
  }

}
