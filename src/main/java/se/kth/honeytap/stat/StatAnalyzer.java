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
import java.util.LinkedList;
import java.util.Queue;
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
  static String resultPath = "17June/wl2/10sec_window_4_noBilling/";
  static String wl1 = "0.5:1, 5:2, 5:3, 5:4, 8:5, 1:4, 1:3, 1:2, 0.5:1";
  static String wl2 = "1:1, 1:2, 1:3, 1:2, 1:3, 1:3, 1:2, 1:3, 1:2, 1:2, 1:3, 1:3, 1:2, 1:2, 1:1, 1:2, 1:2, 1:3, 1:2, 1:1, 1:3, 1:3, 1:2, 1:3, 1:2";

  public static void main(String[] args) throws Exception {
    writeFinalResults();
  }

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
  }


  public static void writeFinalResults() throws Exception {

    BufferedReader br;
    PrintWriter writer;
    Queue<Integer> originalReqQueue = getOriginalReq();
    try {
      String line = null;
      br = new BufferedReader(new FileReader("tupleValues.txt"));
      writer = new PrintWriter(resultPath.concat("finalResult.csv"), "UTF-8");
      ResultTuple previousTuple = new ResultTuple("6/17/2016 16:06:14", 1, 1, 1, 55.0f);  //time,withFeedbackReq,alloc,originalReq
      float previousUtilization = 55.0f;

      while ((line = br.readLine()) != null) {
        String[] elements = line.split(",");
        String timeString = elements[0].trim();
        String utilizationString = elements[1].trim();
        String allocationString = elements[2].trim();

        String time;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date resultDate = new Date(Long.valueOf(timeString));
        time = sdf.format(resultDate);

        if (time.equals(previousTuple.time)) {
          continue;      //TODO may be the previous one should be replaced with new one
        }

        int withFeedbackReq;
        int alloc = Integer.valueOf(allocationString);
        int originalReq = originalReqQueue.poll();

       /* float utilization = Float.valueOf(utilizationString);
        utilization = Float.parseFloat(String.format("%.2f", utilization));
        if ( (utilization >= 30 && utilization <= 80) || (previousTuple.utilization == utilization && previousTuple.originalReq == originalReq) ) {
          withFeedbackReq = previousTuple.withFeedbackReq;
        } else if (utilization > 80) {
          withFeedbackReq = (int) Math.ceil((utilization/80) * alloc);
        } else {
          if (previousUtilization < 30) { // we have already reduced the machine
            withFeedbackReq = Integer.valueOf(previousTuple.withFeedbackReq);
          } else {
            withFeedbackReq = Integer.valueOf(previousTuple.withFeedbackReq) -1;
          }
        }*/

       /* float utilization = Float.valueOf(utilizationString);
        utilization = Float.parseFloat(String.format("%.2f", utilization));   //limiting to two decimal points
        if (previousTuple.utilization == utilization && previousTuple.originalReq == originalReq) {
          withFeedbackReq = previousTuple.withFeedbackReq;
        } else if (utilization >= 30) {
          withFeedbackReq = (int) Math.ceil((utilization/80) * alloc);
        } else {
          if (previousUtilization < 30) { // we have already reduced the machine
            withFeedbackReq = Integer.valueOf(previousTuple.withFeedbackReq);
          } else {
            withFeedbackReq = Integer.valueOf(previousTuple.withFeedbackReq) -1;
          }
        }*/

        float utilization = Float.valueOf(utilizationString);
        utilization = Float.parseFloat(String.format("%.2f", utilization));   //limiting to two decimal points
        if (previousTuple.utilization == utilization && previousTuple.originalReq == originalReq) {
          withFeedbackReq = previousTuple.withFeedbackReq;
        } else if (utilization >= 30 && utilization <= 80) {
          if (originalReq == alloc) {
            withFeedbackReq = alloc;
          } else {
            withFeedbackReq = (int) Math.ceil((utilization/80) * alloc);
          }
        } else if (utilization > 80) {
          withFeedbackReq = (int) Math.ceil((utilization/80) * alloc);
        } else {
          if (previousUtilization < 30) { // we have already reduced the machine
            withFeedbackReq = Integer.valueOf(previousTuple.withFeedbackReq);
          } else {
            withFeedbackReq = Integer.valueOf(previousTuple.withFeedbackReq) -1;
          }
        }

        /////////TODO set original req here
        ResultTuple newTuple = new ResultTuple(time, withFeedbackReq, alloc, originalReq, utilization);
        previousTuple = newTuple;
        previousUtilization = utilization;
        writer.println(newTuple.utilization + "," + newTuple.time + "," + newTuple.withFeedbackReq + "," +
                newTuple.allocation + "," + newTuple.originalReq);
      }
      writer.flush();
      br.close();

    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    log.info("==========================================================");
    log.info("==========================================================");
    log.info("================== Stored final output ===================");
    log.info("==========================================================");
    log.info("==========================================================");
  }

  private static Queue<Integer> getOriginalReq() throws Exception {
    Queue<Integer> reqQueue = new LinkedList<>();

    //originalReqDef:    0.5:1, 5:2, 5:3, 5:4, 8:5, 1:4, 1:3, 1:2, 0.5:1
    String originalReqDef = "";
    if (resultPath.contains("wl1")) {
      originalReqDef = wl1;
    } else if (resultPath.contains("wl2")) {
      originalReqDef = wl2;
    } else {
      throw new Exception("Couldnt find the matching workload");
    }
    String[] reqArray = originalReqDef.split(",");
    for (String req : reqArray) {
      String durationString = req.trim().split(":")[0];
      int currentReq = Integer.valueOf(req.trim().split(":")[1]);
      int durationMin = 0;
      int durationSec = 0;
      if (durationString.contains(".")) {
        //will have both min and sec
        durationMin = Integer.valueOf(durationString.split("\\.")[0]);
        durationSec = Integer.valueOf(durationString.split("\\.")[1]);
      } else {
        durationMin = Integer.valueOf(durationString);
      }

      if (durationMin > 0) {
        for (int i = 0; i < durationMin * 60; i ++) {
          reqQueue.add(currentReq);
        }
      }
      if (durationSec > 0) {
        for (int i = 0; i < durationSec; i ++) {
          reqQueue.add(currentReq);
        }
      }
    }
    return reqQueue;
  }

  static class ResultTuple {
    String time;
    int withFeedbackReq;
    int allocation;
    int originalReq;
    float utilization;

    public ResultTuple(String time, int withFeedbackReq, int allocation, int originalReq, float utilization) {
      this.time = time;
      this.withFeedbackReq = withFeedbackReq;
      this.allocation = allocation;
      this.originalReq = originalReq;
      this.utilization = utilization;
    }
  }
}