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
  static String experimentPath = "experiments/June_22/predict/wl3/wl3_Off_wi10sec_out50sec_in10sec_new/";
  static String resultPath = experimentPath.concat("results/");
  static String filteredPath = experimentPath.concat("filtered/");
  //static String wl0 = "0.35:1, 1.15:2, 1.15:3, 1.15:4, 1.15:5, 4:6";  // 10sec, 20sec, 35sec, 50sec, 100sec
  static String wl0 = "0.15:1, 0.35:2, 0.40:2, 0.35:3, 0.40:3, 0.35:4, 0.40:4, 0.35:5, 0.40:5, 0.35:6, 3.25:6";  // 10sec, 20sec, 35sec, 50sec, 100sec
  static String wl1 = "0.35:1, 1.15:2, 1.15:3, 1.15:4, 4:5";  // 10sec, 1min, 2min
  static String wl2 = "0.13:5, 0.6:4, 0.6:3, 0.6:2, 1:1";   //3,8,14 seconds
  //static String wl3 = "1:1, 1:2, 1:3, 1:2, 1:3, 1:3, 1:2, 1:3, 1:2, 1:2, 1:3, 1:4, 1:2, 1:1, 1:2";   //10:1, 20:2, 35:3,50:5,100:10
  static String wl3 = "0.40:1, 0.20:2, 0.40:2, 0.20:3, 0.40:3, 1:2, 0.20:3, 0.40:3, 1:3, 1:2, 0.20:3, 0.40:3, 1:2, 1:2, 0.20:3, 0.40:3, 0.2:4, 0.3:4, 1:2, 1:1, 0.20:2, 0.40:2";   //10:1, 20:2, 35:3,50:5,100:10
  //static String wlx = "1:1, 0.71: 1:2, 1:3, 1:2, 1:3, 1:3, 1:2, 1:3, 1:2, 1:2, 1:3, 1:4, 1:2, 1:1, 1:2";   //10:1, 20:2, 35:3,50:5,100:10
  static String wlx = "0.45:1, 0.15:2, 0.45:2, 0.15:3, 0.45:3, 0.15:2, 0.45:2, 0.15:3, 0.45:3, 0.15:3, 0.45:3, 0.15:2, 0.45:2, 0.15:3, 0.45:3, 0.15:2,  0.45:2, 0.15:2, 0.45:2, 0.15:3, 0.45:3, 0.15:4, 0.45:4, 0.15:2, 0.45:2, 0.15:1, 0.45:1, 1:2";   //10:1, 20:2, 35:3,50:5,100:10
  static String wl4 = "3:1, 3:2, 3:3, 3:2, 3:3, 3:3, 3:2, 3:3, 3:2, 3:2, 3:3, 0.5:4, 3:2, 3:1, 3:2";   //10:1, 20:2, 35:3,50:5,100:10

  public static void main(String[] args) throws Exception {
    writeFinalResults();
  }

  {
    new File(resultPath).mkdir();
    new File(filteredPath).mkdir();
  }

  public static void analyze() {
    try {

      File resultDir = new File(resultPath);

      if (!resultDir.exists()) {
        throw new Exception("Results are not available in " + resultDir.getAbsolutePath());
      }

      ////FileUtils.copyDirectory(new File("filtered"), resultDir);
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
      writeFinalResults();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    } catch (Exception e) {
      log.error("Failed to write final results?? ");
      e.printStackTrace();
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
      /////FileUtils.copyFile(new File("tupleValues.txt"), new File(experimentPath.concat("tupleValues.txt")));
      br = new BufferedReader(new FileReader(experimentPath.concat("tupleValues.txt")));
      writer = new PrintWriter(experimentPath.concat("finalResult.csv"), "UTF-8");
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
        /////////////if (previousTuple.utilization == utilization && previousTuple.originalReq == originalReq && utilization <= 100) {
        if (previousTuple.utilization == utilization && previousTuple.originalReq == originalReq) {
          //this condition is used to avoid the situations where the resources are allocated and the resource utilization
          //is not updated yet since we update one variable in a tuple at a time (if we use the same equation of ceil(..)
          //since the allocation is increased and utilization has not decreased yet, withFeedbackReq end up having a very high value
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
          //if (previousUtilization < 30) { // we have already reduced the machine
          if (alloc > originalReq) {
            withFeedbackReq = originalReq;
          } else if (previousUtilization == utilization) { // we have already reduced the machine
            withFeedbackReq = previousTuple.withFeedbackReq;
          } else {
            if (previousTuple.withFeedbackReq > 1) {
              withFeedbackReq = previousTuple.withFeedbackReq -1;
            } else {
              withFeedbackReq = previousTuple.withFeedbackReq; //won't reduce than 1 machine
            }
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

   /* SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date resultDate = new Date(Long.valueOf(timeString));
    time = sdf.format(resultDate);*/

    //originalReqDef:    0.5:1, 5:2, 5:3, 5:4, 8:5, 1:4, 1:3, 1:2, 0.5:1
    String originalReqDef = "";
    if (experimentPath.contains("wl0")) {
      originalReqDef = wl0;
    } else if (experimentPath.contains("wl1")) {
      originalReqDef = wl1;
    } else if (experimentPath.contains("wl2")) {
      originalReqDef = wl2;
    } else if (experimentPath.contains("wl3")) {
      originalReqDef = wl3;
    } else if (experimentPath.contains("wlx")) {
      originalReqDef = wlx;
    }  else if (experimentPath.contains("wl4")) {
      originalReqDef = wl4;
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