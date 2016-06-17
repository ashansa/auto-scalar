package se.kth.honeytap.stat;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeMap;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class StatAnalyzer {
  static Log log = LogFactory.getLog(StatAnalyzer.class);
  static String resultPath = "17June/wl1/10sec_window_2/";

  public static void analyze() {
    try {

      File resultDir = new File(resultPath);

      if (!new File(resultPath).exists()) {
        new File(resultPath).mkdirs();
      }

      FileUtils.copyDirectory(new File("filtered"), resultDir);
      PrintWriter writer = new PrintWriter(resultPath.concat("all.txt"), "UTF-8");
      for (File file : resultDir.listFiles()) {
        if (!".DS_Store".equals(file.getName())) {
          BufferedReader br;

          try {
            String line = null;
            br = new BufferedReader(new FileReader(file));

            while ((line = br.readLine()) != null) {
              writer.println(line);
            }
            writer.flush();
            br.close();
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
        }
      }
      sortAllVals();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static void sortAllVals() {
    BufferedReader br;
    PrintWriter writer;
    try {
      String line = null;
      br = new BufferedReader(new FileReader(resultPath.concat("all.txt")));
      writer = new PrintWriter(resultPath.concat("allSorted.txt"), "UTF-8");
      float prev = 0;
      TreeMap<Long, String> valueMap = new TreeMap<>();
      while ((line = br.readLine()) != null) {
        Long key = Long.valueOf(line.split("=")[0]);
        String val = line.split("=")[1];
        if (valueMap.containsKey(key)) {
          val = valueMap.get(key).concat(",").concat(val);
        }
        valueMap.put(key, val);
      }

      for (Long timeKey : valueMap.keySet()) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date resultDate = new Date(timeKey);
        String time = sdf.format(resultDate);
        String value = valueMap.get(timeKey);
        if (value.contains(",")) {
          String[] values = value.split(",");
          for (String val : values) {
            writer.println((time) + "=" + val);
          }
        } else {
          writer.println((time) + "=" + value );
        }
      }
      writer.flush();
      br.close();

      //creating the sorted list with the difference of milliseconds
      /*Long firstTime = valueMap.firstKey();

      for (Long timeKey : valueMap.keySet()) {
        String value = valueMap.get(timeKey);
        if (value.contains(",")) {
          String[] values = value.split(",");
          for (String val : values) {
            writer.println((timeKey - firstTime) + "=" + val);
          }
        } else {
          writer.println((timeKey - firstTime) + "=" + value );
        }
      }
      writer.flush();
      br.close();*/
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    log.info("==========================================================");
    log.info("==========================================================");
    log.info("================== logging data done===================");
    log.info("==========================================================");
    log.info("==========================================================");
  }
}