package com.elex.bigdata.historytracer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * User: Z J Wu Date: 14-1-16 Time: 下午2:07 Package: com.elex.bigdata.historytracer
 */
public class FileUtils {
  private static final Logger LOGGER = Logger.getLogger(FileUtils.class);

  public static boolean copy2HDFS(String localPath, String hdfsPath) throws IOException {
    File f = new File(localPath);
    if (!f.exists()) {
      LOGGER.error("[FILE-UTILS] - Local file (" + localPath + ") does not exist.");
      return false;
    }

    Path srcPath = new Path(localPath), destPath = new Path(hdfsPath);
    FileSystem fileSystem = FileSystem.get(new Configuration());
    fileSystem.copyFromLocalFile(false, true, srcPath, destPath);
    return true;
  }
}
