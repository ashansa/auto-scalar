package se.kth.honeytap.stat;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class StatManager {

  static HashMap<Long,String> ramChanges = new HashMap<>();   //high:v1, low:v2  , normal:v3
  static HashMap<Long,String> cuChanges = new HashMap<>();   //high:v1, low:v2  , normal:v3
  static HashMap<Long,String> machineReq = new HashMap<>();   //2,  3,  4,  3
  static HashMap<Long,String> machineAllocation = new HashMap<>();     //1:t2.medium  ,   -1,Id2

  public static void addRamChanges(Long key, String level, float value) {
    String valueSet = "";
    if (ramChanges.containsKey(key)) {
      valueSet = ramChanges.get(key).concat(",");
    }
    String newValue = level + ":" + value;
    valueSet = valueSet.concat(newValue);
    ramChanges.put(key, valueSet);
  }

  public static void addCuChanges(Long key, String level, float value) {
    String valueSet = "";
    if (cuChanges.containsKey(key)) {
      valueSet = cuChanges.get(key).concat(",");
    }
    String newValue = level + ":" + value;
    valueSet = valueSet.concat(newValue);
    cuChanges.put(key, valueSet);
  }

  public static void setMachineReq(Long key, int no) {
    String valueSet = "";
    if (machineReq.containsKey(key)) {
      valueSet = machineReq.get(key).concat(",");
    }
    valueSet = valueSet + no;
    machineReq.put(key, valueSet);
  }

  public static void setMachineAllocation(Long key, int no, String option) {
    String valueSet = "";
    if (machineAllocation.containsKey(key)) {
      valueSet = machineAllocation.get(key).concat(",");
    }
    String newValue = no + ":" + option;
    valueSet = valueSet.concat(newValue);
    machineAllocation.put(key, valueSet);
  }

  public static void storeValues() {
    Properties ramProperties = new Properties();
    Properties cuProperties = new Properties();
    Properties machineReqProperties = new Properties();
    Properties machineAllocProperties = new Properties();

    for (Map.Entry<Long,String> entry : ramChanges.entrySet()) {
      ramProperties.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<Long,String> entry : cuChanges.entrySet()) {
      cuProperties.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<Long,String> entry : machineReq.entrySet()) {
      machineReqProperties.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<Long,String> entry : machineAllocation.entrySet()) {
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
}
