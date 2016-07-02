package se.kth.honeytap.stat;

import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class StatManager {

  static TreeMap<String,String> ramChanges = new TreeMap<>();   //high:v1, low:v2  , normal:v3
  static TreeMap<String,String> cuChanges = new TreeMap<>();   //high:v1, low:v2  , normal:v3
  static TreeMap<String,String> machineReq = new TreeMap<>();   //2,  3,  4,  3
  static TreeMap<String,String> machineAllocation = new TreeMap<>();     //1:t2.medium  ,   -1,Id2

  static TreeMap<Long, ArrayList<Tuple>> changes = new TreeMap<>();
  static ReentrantLock lock = new ReentrantLock();
  static Tuple previousTuple = new Tuple(System.currentTimeMillis(), 55.0f, 1);

  public static void main(String[] args) throws IOException {
    filterNStore();
  }

  public static void addRamChanges(Long key, String level, float value) {
    String valueSet = "";
    String keyString = String.valueOf(key);
    if (ramChanges.containsKey(keyString)) {
      valueSet = ramChanges.get(keyString).concat(",");
    }
    String newValue = level + ":" + value;
    valueSet = valueSet.concat(newValue);
    ramChanges.put(keyString, valueSet);

    lock.lock();
    ArrayList<Tuple> tupleArray = new ArrayList<>();
    Tuple newTuple;

    if (changes.containsKey(key)) {
      tupleArray = changes.get(key);
    }

    if (previousTuple != null) {
      newTuple = new Tuple(key, value, previousTuple.noOfMachines);
    } else {
      newTuple = new Tuple(key, value, 1);
    }

    tupleArray.add(newTuple);
    previousTuple = newTuple;
    changes.put(key, tupleArray);
    lock.unlock();

  }

  public static void addCuChanges(Long key, String level, float value) {
    String valueSet = "";
    String keyString = String.valueOf(key);
    if (cuChanges.containsKey(keyString)) {
      valueSet = cuChanges.get(keyString).concat(",");
    }
    String newValue = level + ":" + value;
    valueSet = valueSet.concat(newValue);
    cuChanges.put(String.valueOf(keyString), valueSet);
  }

  public static void setMachineReq(Long key, int no) {
    String valueSet = "";
    String keyString = String.valueOf(key);
    if (machineReq.containsKey(keyString)) {
      valueSet = machineReq.get(keyString).concat(",");
    }
    valueSet = valueSet + no;
    machineReq.put(String.valueOf(keyString), valueSet);
  }

  public static void setMachineAllocation(Long key, int no, String option) {
    String valueSet = "";
    String keyString = String.valueOf(key);
    if (machineAllocation.containsKey(keyString)) {
      valueSet = machineAllocation.get(keyString).concat(",");
    }
    String newValue = no + ":" + option;
    valueSet = valueSet.concat(newValue);
    machineAllocation.put(keyString, valueSet);

    lock.lock();
    ArrayList<Tuple> tupleArray = new ArrayList<>();
    Tuple newTuple;
    if (changes.containsKey(key)) {
      tupleArray = changes.get(key);
    }

    if (previousTuple != null) {
      newTuple = new Tuple(key, previousTuple.ramUtilization, previousTuple.noOfMachines + no);
    } else {
      newTuple = new Tuple(key, 0.0000000000f, no);
    }

    tupleArray.add(newTuple);
    previousTuple = newTuple;
    changes.put(key, tupleArray);
    lock.unlock();
  }

  public static void storeValuesBk() {
    Properties ramProperties = new Properties();
    Properties cuProperties = new Properties();
    Properties machineReqProperties = new Properties();
    Properties machineAllocProperties = new Properties();

    for (Map.Entry<String,String> entry : ramChanges.entrySet()) {
      ramProperties.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String,String> entry : cuChanges.entrySet()) {
      cuProperties.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String,String> entry : machineReq.entrySet()) {
      machineReqProperties.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String,String> entry : machineAllocation.entrySet()) {
      machineAllocProperties.put(entry.getKey(), entry.getValue());
    }

    try {
      ramProperties.store(new FileOutputStream("ramChanges.properties"), null);
      cuProperties.store(new FileOutputStream("cuChanges.properties"), null);
      machineReqProperties.store(new FileOutputStream("machineReq.properties"), null);
      machineAllocProperties.store(new FileOutputStream("machineAlloc.properties"), null);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static void storeValues() throws IOException {
    PrintWriter writer = null;
    try {
      writer = new PrintWriter("ramChanges.txt", "UTF-8");
      for (Map.Entry<String,String> entry : ramChanges.entrySet()) {
        writer.println(entry.getKey() + "=" + entry.getValue());
      }
      writer.flush();
      writer.close();

      writer = new PrintWriter("cuChanges.txt", "UTF-8");
      for (Map.Entry<String,String> entry : cuChanges.entrySet()) {
        writer.println(entry.getKey() + "=" + entry.getValue());
      }
      writer.flush();
      writer.close();

      writer = new PrintWriter("machineReq.txt", "UTF-8");
      for (Map.Entry<String,String> entry : machineReq.entrySet()) {
        writer.println(entry.getKey() + "=" + entry.getValue());
      }
      writer.flush();
      writer.close();

      writer = new PrintWriter("machineAlloc.txt", "UTF-8");
      for (Map.Entry<String,String> entry : machineAllocation.entrySet()) {
        writer.println(entry.getKey() + "=" + entry.getValue());
      }
      writer.flush();
      writer.close();

      writer = new PrintWriter("tupleValues.txt", "UTF-8");
      for (ArrayList<Tuple> tupleArray : changes.values()) {
        for (Tuple tuple : tupleArray) {
          writer.println(tuple.time + ", " + tuple.ramUtilization + ", " + tuple.noOfMachines);
        }
      }
      writer.flush();
      writer.close();

    } catch (FileNotFoundException e) {
      throw new IllegalStateException(e);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
    filterNStore();
  }

  public static void filterNStore() throws IOException {

    FileUtils.copyFile(new File("ramChanges.txt"), new File(StatAnalyzer.resultPath.concat("ramChanges.txt")));
    FileUtils.copyFile(new File("cuChanges.txt"), new File(StatAnalyzer.resultPath.concat("cuChanges.txt")));
    FileUtils.copyFile(new File("machineReq.txt"), new File(StatAnalyzer.resultPath.concat("machineReq.txt")));
    FileUtils.copyFile(new File("machineAlloc.txt"), new File(StatAnalyzer.resultPath.concat("machineAlloc.txt")));
    FileUtils.copyFile(new File("tupleValues.txt"), new File(StatAnalyzer.experimentPath.concat("tupleValues.txt")));

    BufferedReader br;
    PrintWriter writer;
    if (!new File(StatAnalyzer.filteredPath).exists()) {
      new File(StatAnalyzer.filteredPath).mkdir();
    }
    try {
      String line = null;
      br = new BufferedReader(new FileReader(StatAnalyzer.resultPath.concat("ramChanges.txt")));
      writer = new PrintWriter(StatAnalyzer.filteredPath.concat("ramChangesFiltered.txt"), "UTF-8");
      float prev = 0;
      while ((line = br.readLine()) != null) {   //1465949322168=high:82.5
        //float value = Float.valueOf(line.split(":")[1]);
        String valueString = line.split(":")[1];
        if (valueString.contains(",")) {
          valueString = line.split("=")[1];
          String[] eventVals = valueString.split(",");
          for (String eventVal : eventVals) {
            float value = Float.valueOf(eventVal.split(":")[1]);
            if (value != prev) {
              writer.println(line.split("=")[0] + "=" + eventVal);
              prev = value;
            }
          }
        } else {
          float value = Float.valueOf(line.split(":")[1]);
          if (value != prev) {
            writer.println(line);
            prev = value;
          }
        }
      }
      writer.flush();
      br.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    try {
      String line = null;
      br = new BufferedReader(new FileReader(StatAnalyzer.resultPath.concat("cuChanges.txt")));
      writer = new PrintWriter(StatAnalyzer.filteredPath.concat("cuChangesFiltered.txt"), "UTF-8");
      float prev = 0;
      while ((line = br.readLine()) != null) {   //1465949206171=normal:55.0
        /*float value = Float.valueOf(line.split(":")[1]);
        if ( value != prev) {
          writer.println(line);
          prev = value;
        }*/
        String valueString = line.split(":")[1];
        if (valueString.contains(",")) {
          valueString = line.split("=")[1];
          String[] eventVals = valueString.split(",");
          for (String eventVal : eventVals) {
            float value = Float.valueOf(eventVal.split(":")[1]);
            if (value != prev) {
              writer.println(line.split("=")[0] + "=" + eventVal);
              prev = value;
            }
          }
        } else {
          float value = Float.valueOf(line.split(":")[1]);
          if (value != prev) {
            writer.println(line);
            prev = value;
          }
        }
      }
      writer.flush();
      br.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    try {
      String line = null;
      br = new BufferedReader(new FileReader(StatAnalyzer.resultPath.concat("machineReq.txt")));
      writer = new PrintWriter(StatAnalyzer.filteredPath.concat("machineReqFiltered.txt"), "UTF-8");
      float prev = 0;
      while ((line = br.readLine()) != null) {   //1465951148117=2
        float value = Float.valueOf(line.split("=")[1]);
        if ( value != prev) {
          writer.println(line);
          prev = value;
        }
      }
      writer.flush();
      br.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    try {
      String line = null;
      br = new BufferedReader(new FileReader(StatAnalyzer.resultPath.concat("machineAlloc.txt")));
      writer = new PrintWriter(StatAnalyzer.filteredPath.concat("machineAllocFiltered.txt"), "UTF-8");

      while ((line = br.readLine()) != null) {   //1465951245253=1:t2.medium
        writer.println(line);
      }
      writer.flush();
      br.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    StatAnalyzer.analyze();
  }

  static class Tuple {
    long time;
    float ramUtilization;
    int noOfMachines;

    public Tuple(long time, float ramUtilization, int noOfMachines) {
      this.time = time;
      this.ramUtilization = ramUtilization;
      this.noOfMachines = noOfMachines;
    }
  }
}