package se.kth.honeytap.stat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.TreeMap;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class StatAnalyzer {
  public static void main(String[] args) {
    sortAllVals();
  }

  public static void sortAllVals() {
    BufferedReader br;
    PrintWriter writer;
    try {
      String line = null;
      br = new BufferedReader(new FileReader("a_results/10sec_window_1/all.txt"));
      writer = new PrintWriter("a_results/10sec_window_1/allSorted.txt", "UTF-8");
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

      /*long yourmilliseconds = System.currentTimeMillis();
      SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm");
      Date resultdate = new Date(yourmilliseconds);
      System.out.println(sdf.format(resultdate));*/

      //creating the sorted list with the difference of milliseconds
      Long firstTime = valueMap.firstKey();

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
      br.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}