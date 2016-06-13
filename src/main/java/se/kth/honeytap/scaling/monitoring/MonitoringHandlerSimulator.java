package se.kth.honeytap.scaling.monitoring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.Constants;
import se.kth.honeytap.scaling.core.HoneyTapAPI;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MonitoringHandlerSimulator implements MonitoringHandler{

  Log log = LogFactory.getLog(MonitoringHandlerSimulator.class);
  private HoneyTapAPI honeyTapAPI;
  MonitoringListener monitoringListener;
  Map<String, EventProducer> producerMap = new HashMap<>();
  private ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2); //to handle >= and <= ?? or just to be safe??

  public MonitoringHandlerSimulator(HoneyTapAPI honeyTapAPI) {
    this.honeyTapAPI = honeyTapAPI;
    this.monitoringListener = new MonitoringListener(this.honeyTapAPI);
  }

  public MonitoringListener addGroupForMonitoring(String groupId, InterestedEvent[] interestedEvents) throws HoneyTapException {
    //TODO stimulator will consider interested events only with = sign (no lessThan, greaterThan for simulation)
    //lessThan =====will be changed as ====> lessThanOrEqual
    String cuWorkload = "";
    String ramWorkload = "";

    Properties workloadProp = new Properties();
    String workloadFilePath = System.getProperty("user.home").concat(File.separator).concat(
            ".karamel/rules/workload.properties");

    try {
      InputStream is = new FileInputStream(new File((workloadFilePath)));
      log.info("........... reading workload from file.........");
      workloadProp.load(is);
      cuWorkload = workloadProp.getProperty("cpu");
      ramWorkload = workloadProp.getProperty("ram");
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    /*cuWorkload = "10:1.1";  //no of cus needs for each time point (min vcu req: 2)
    //String ramWorkload = "1:1, 5:3, 10:3.7, 5:2.5, 5:10, 10:3.8, 4:1";  (min ram req: 4GB)
    ramWorkload = "1:3, 2:3.5, 3:7.1, 2:1";  //memory GB needes for each time point  (min ram req: 4GB)*/

    EventProducer eventProducer = new EventProducer(groupId, monitoringListener, cuWorkload, ramWorkload, 1);
    producerMap.put(groupId, eventProducer);
    eventProducer.addInitialInterestedEvents(interestedEvents);
    eventProducer.startMonitoring();

    return monitoringListener;
  }

  public void removeGroupFromMonitoring(String groupId) {
    // avoid monitoring and sending events of the machines of the group
    EventProducer producer = producerMap.get(groupId);
    if (producer != null) {
      producer.stopMonitoring();
    }
  }

  public void addInterestedEvent(String groupId, InterestedEvent[] events, int timeDurationSec) {
    log.info("============= addInterestedEvent with duration not yet implemented ============");
    EventProducer producer = producerMap.get(groupId);
    if (producer == null) {
      log.error("No event producer is available for group: " + groupId + " Cannot add events on duration");
    } else {
      TimedEventHandler timedEventHandler = new TimedEventHandler(groupId, events, timeDurationSec, producer);
      executor.execute(timedEventHandler);
    }

    //TODO request events for a limited time
    //should handle all interested event types
    //resource interests: CPU:>=:80; RAM:<=:30;   CPU:AVG:>=:10:<=:90  ; CPU:>=:70:<=:80
    // machine interests: KILLED; AT_END_OF_BILLING_PERIOD

    /*for (InterestedEvent event : events) {
      try {
        String items[] = event.getInterest().split(Constants.SEPARATOR);
        if (event.getInterest().contains(Constants.AVERAGE) && items.length == 6) {  //ie: CPU:AVG:>=:10:<=:90
          tablespoonAPI.createTopic(monitoringListener, groupId, EventType.GROUP_AVERAGE, MonitoringUtil.
                  getMonitoringResourceType(items[0]), timeDurationSec, MonitoringUtil.
                  getMonitoringThreshold(items[4], items[5]), MonitoringUtil.getMonitoringThreshold(
                  items[2], items[3]));
        } else if (items.length == 5) {  //ie: CPU:>=:70:<=:80
          tablespoonAPI.createTopic(monitoringListener, groupId, EventType.REGULAR, MonitoringUtil.
                  getMonitoringResourceType(items[0]), timeDurationSec, MonitoringUtil.getMonitoringThreshold(
                  items[3], items[4]), MonitoringUtil.getMonitoringThreshold(items[1], items[2]));
        }
      }catch(ThresholdException e){
        log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " +
                groupId, e);
      }
    }*/
  }

  public void removeInterestedEvent(String groupId, InterestedEvent events, int timeDuration) {
    //TODO remove events for a limited time
  }

  public void addVmInfo(String groupId, String vmId, int numVCpu, double memInGig, Integer numDisks, Integer diskSize, boolean reset) {
    producerMap.get(groupId).addVMInfo(vmId, numVCpu, memInGig, numDisks, diskSize, reset);
  }

  public void addSimulatedVMInfo(String groupId, String vmId, int numVCpu, double memInGig, Integer numDisks, Integer diskSize) {
    producerMap.get(groupId).addSimulatedVMInfo(vmId, numVCpu, memInGig, numDisks, diskSize);
  }

  public void removeSimulatedVMInfo(String groupId, String vmId) {
    producerMap.get(groupId).removeSimulatedVMInfo(vmId);
  }

  public String[] getAllSimulatedVmIds(String groupId) {
    return producerMap.get(groupId).getAllSimulatedVmIds();
  }

  class EventProducer {

    private boolean isMonitoringActivated;
    private String groupId;
    private MonitoringListener monitoringListener;
    private int monitoringFreqSeconds;   //each 1sec, 2sec, etc
    private Map<String,Queue<Float>> resourceWorkloadMap = new HashMap<>();   //CPU: 12,12,12,12,52,52,.... (monitoring values)

    //ResourceType: Interested threshold Map
    private Map<String, Float> greaterThanInterestMap = new HashMap<>();
    private Map<String, Float> lessThanInterestMap = new HashMap<>();

    //ID: <ResourceType:thresholdLow:thresholdHigh> map
    private Map<String, String> durationGreaterInterestMap = new HashMap<>();
    private Map<String, String> durationLessInterestMap = new HashMap<>();

    private ReentrantLock durationGreaterLock = new ReentrantLock();
    private ReentrantLock durationLessLock = new ReentrantLock();

    private HashMap<String, VM> vmMap = new HashMap<>();
    private HashMap<String, VM> simulatedVmMap = new HashMap<>();

    float initialSystemRam = 3.75f;
    int initialSystemCus = 1;

    //TODO if we need to handle >1 group, need to have a map of groupId-greater/lessThanInterestMap
    //workload

    //spawning delay
    //monitoring events delay??? (AS can't help if monitoring is delayed)

    EventProducer(String groupId, MonitoringListener monitoringListener, String cuWorkload, String ramWorkload, int monitoringFreqSeconds) {
      this.groupId = groupId;
      this.monitoringListener = monitoringListener;
      this.monitoringFreqSeconds = monitoringFreqSeconds;

      //minutes:resource_utilization%
      addWorkload(RuleSupport.ResourceType.CPU.name(), cuWorkload);
      addWorkload(RuleSupport.ResourceType.RAM.name(), ramWorkload); //1 machine range
      isMonitoringActivated = true;
    }

    private void addWorkload(String resourceType, String workload) {
      Queue<Float> workloadQueue = new LinkedList<Float>();

      //workload:    "1:10, 5:50, 10:95, 5:57, 5:180, 10:62, 4:12"
      String[] workloadArray = workload.split(",");
      for (String wl : workloadArray) {
        int durationMin = Integer.valueOf(wl.trim().split(":")[0]);
        float utilization = Float.valueOf(wl.trim().split(":")[1]);
        for (int i = 0; i < durationMin * 60; i = i + monitoringFreqSeconds) {
          workloadQueue.add(utilization);
        }
      }
      resourceWorkloadMap.put(resourceType, workloadQueue);
    }

    /*class WLTuple {
      int minuteInstance;
      float workload;

      public WLTuple(int minuteInstance, float workload) {
        this.minuteInstance = minuteInstance;
        this.workload = workload;
      }
    }*/

    public void addInitialInterestedEvents(InterestedEvent[] interestedEvents) {
      for (InterestedEvent interestedEvent : interestedEvents) {
        //3 forms of items   RAM:<=:30;   CPU:AVG:>=:10:<=:90;    KILLED;
        //but the initial form will be always the 1st form
        try {
          String[] items = interestedEvent.getInterest().split(Constants.SEPARATOR);
          if (items.length != 3) {
            if (MachineMonitoringEvent.Status.KILLED.name().equals(interestedEvent.getInterest()) ||
                    MachineMonitoringEvent.Status.AT_END_OF_BILLING_PERIOD.name().equals(interestedEvent.getInterest())) {

              log.info("AT_END_OF_BILLING_PERIOD and KILLED events will be handled by Karamel. So will not add to " +
                      "simulator");
              continue;
            } else {
              throw new Exception("Interested event is not in the correct format :" + interestedEvent.getInterest());
            }
          }
          RuleSupport.Comparator unifiedComparator = RuleSupport.getNormalizedComparatorType(
                  RuleSupport.Comparator.valueOf(items[1]));

          Float definedThreshold = Float.valueOf(items[2]);

          if (RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.equals(unifiedComparator)) {
            Float existingValue = greaterThanInterestMap.get(items[1]);
            if (existingValue == null) {
              greaterThanInterestMap.put(items[0], definedThreshold);
            } else {
              //since greater than, go for the lowest threshold
              if (definedThreshold < existingValue) {
                greaterThanInterestMap.put(items[0], definedThreshold);
              } //else no need to change since new value includes in existing threshold
            }
          } else { //this is lessThanOrEqual part
            Float existingValue = lessThanInterestMap.get(items[1]);
            if (existingValue == null) {
              lessThanInterestMap.put(items[0], definedThreshold);
            } else {
              //since less than, go for the highest threshold
              if (definedThreshold > existingValue) {
                lessThanInterestMap.put(items[0], definedThreshold);
              } //else no need to change since new value includes in existing threshold
            }
          }

        } catch (Exception e) {
          log.warn("Could not add the monitoring event: " + interestedEvent.getInterest() + " for group: " +
                  groupId, e);
        }
      }
    }

    public void stopMonitoring() {
      log.info("========= going to stop monitoring of group: " + groupId);
      isMonitoringActivated = false;
    }

    class MonitoringTimer extends TimerTask {
      Queue<Float> ramWorkload;
      Queue<Float> cuWorkload;
      float remainingCuReq = 0;
      float remainingRamReq = 0;

      public MonitoringTimer() {
        ramWorkload = resourceWorkloadMap.get(RuleSupport.ResourceType.RAM.name());
        cuWorkload = resourceWorkloadMap.get(RuleSupport.ResourceType.CPU.name());
      }

      @Override
      public void run() {

        if (isMonitoringActivated) {
          ResourceMonitoringEvent resourceMonitoringEvent;

          float originalRamReq = ramWorkload.poll();
          float ramRequirement = originalRamReq + remainingRamReq;
           /* int noOfGBsInSys = 4; //TODO-get this from Karamel API
            if (ramRequirement > 4 )
              noOfGBsInSys = 8;    //TODO dynamically update with Karamel API*/
          float noOfGBsInSys = getTotalRamInGroup();

          float ramUtilization = ramRequirement/noOfGBsInSys * 100; //TODO get this for each machine to send utilization events

          Float highRamThreshold = greaterThanInterestMap.get(RuleSupport.ResourceType.RAM.name());
          Float lowRamThreshold = lessThanInterestMap.get(RuleSupport.ResourceType.RAM.name());

          float originalCuReq = cuWorkload.poll();
          float cuRequirement = originalCuReq + remainingCuReq;

          ////int noOfCUsInSys = 2; //TODO-get this from Karamel API
          int noOfCUsInSys = getTotalCusInGroup();

          float cpuUtilization = cuRequirement/noOfCUsInSys * 100; //TODO get this for each machine to send utilization events

          Float highCpuThreshold = greaterThanInterestMap.get(RuleSupport.ResourceType.CPU.name());
          Float lowCpuThreshold = lessThanInterestMap.get(RuleSupport.ResourceType.CPU.name());

          for (String resThresholdsString : durationGreaterInterestMap.values()) {
            String[] items = resThresholdsString.split(":"); //ie: CPU:70:80
            if (items.length == 3) {
              if (RuleSupport.ResourceType.RAM.name().equals(items[0])) {
                //I am just assigning the lower value here since we always add timed events a delta less than original interest
                highRamThreshold = Float.valueOf(items[1]);
              } else if (RuleSupport.ResourceType.CPU.name().equals(items[0])) {
                //I am just assigning the lower value here since we always add timed events a delta less than original interest
                highCpuThreshold = Float.valueOf(items[1]);
              }
            }
          }

          for (String resThresholdsString : durationLessInterestMap.values()) {
            String[] items = resThresholdsString.split(":"); //ie: CPU:20:30
            if (items.length == 3) {
              if (RuleSupport.ResourceType.RAM.name().equals(items[0])) {
                //I am just assigning the lower value here since we always add timed events a delta less than original interest
                lowRamThreshold = Float.valueOf(items[2]);
              } else if (RuleSupport.ResourceType.CPU.name().equals(items[0])) {
                //I am just assigning the lower value here since we always add timed events a delta less than original interest
                lowCpuThreshold = Float.valueOf(items[2]);
              }
            }
          }

          /*  log.info("++++++++++++ new RAM interests, high:low ++++++++++++" + highRamThreshold + ":" + lowRamThreshold);
            log.info("++++++++++++ new CPU interests, high:low ++++++++++++" + highCpuThreshold + ":" + lowCpuThreshold);*/

          boolean eventSent = false;
          if (highRamThreshold != null && ramUtilization >= highRamThreshold) {
            //TODO get machine Ids and send values for all machine Ids
            resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.RAM,
                    RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, ramUtilization);
            try {
              log.info("....... going to add onHighRam event. Utilization: " + resourceMonitoringEvent.getCurrentValue());
              monitoringListener.onHighRam(groupId, resourceMonitoringEvent);
              eventSent = true;
            } catch (HoneyTapException e) {
              log.error("Error while sending onHighRam event for groupId: " + groupId + " machine: ", e);
            }
          } else if (lowRamThreshold != null && ramUtilization <= lowRamThreshold) {
            resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.RAM,
                    RuleSupport.Comparator.LESS_THAN_OR_EQUAL, ramUtilization);
            try {
              log.info("....... going to add onLowRam event. Utilization: " + resourceMonitoringEvent.getCurrentValue());
              monitoringListener.onLowRam(groupId, resourceMonitoringEvent);
              eventSent = true;
            } catch (HoneyTapException e) {
              log.error("Error while sending onLowRam event for groupId: " + groupId + " machine: ", e);
            }
          }

          if (highCpuThreshold != null && cpuUtilization >= highCpuThreshold) {
            resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.CPU,
                    RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, cpuUtilization);
            try {
              log.info("....... going to add onHighCPU event. Utilization: " + resourceMonitoringEvent.getCurrentValue());
              monitoringListener.onHighCPU(groupId, resourceMonitoringEvent);
              eventSent = true;
            } catch (HoneyTapException e) {
              log.error("Error while sending onHighCPU event for groupId: " + groupId + " machine: ", e);
            }
          } else if (lowCpuThreshold != null && cpuUtilization <= lowCpuThreshold) {
            resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.CPU,
                    RuleSupport.Comparator.LESS_THAN_OR_EQUAL, cpuUtilization);
            try {
              log.info("....... going to add onHighRam event. Utilization: " + resourceMonitoringEvent.getCurrentValue());

              monitoringListener.onLowCPU(groupId, resourceMonitoringEvent);
              eventSent = true;
            } catch (HoneyTapException e) {
              log.error("Error while sending onLowCPU event for groupId: " + groupId + " machine: ", e);
            }
          }
          if (!eventSent) {
            log.info(".................. no events occured .......... Cpu, Ram ........." + cpuUtilization + "," +
                    ramUtilization);
          }

          if (ramRequirement - noOfGBsInSys > 0) {
            remainingRamReq = ramRequirement - noOfGBsInSys;
          } else {
            remainingRamReq = 0;
          }
          if (cuRequirement - noOfCUsInSys > 0) {
            remainingCuReq = cuRequirement - noOfCUsInSys;
          } else {
            remainingCuReq = 0;
          }
          log.info("&&&&&&&&&&&&&& remaining cu, ram &&&&&&&&&&& " + remainingCuReq + ", " + remainingRamReq);
        }
      }
    }

    public void startMonitoring() {
      long startedTime = System.currentTimeMillis();
      log.info("=========== monitoring of group: " + groupId + " started at: " + startedTime);

      final Timer timer = new Timer();
      timer.scheduleAtFixedRate(new MonitoringTimer(),0, monitoringFreqSeconds * 1000);

    }

    public void startMonitoringBk() {
      //send events periodically considering the workload and the interested events
      //ie ramWorkload = "1:1, 5:3, 10:3.7, 5:2.5, 5:10, 10:3.8, 4:1";  min:utilization
      final Queue<Float> ramWorkload = resourceWorkloadMap.get(RuleSupport.ResourceType.RAM.name());
      final Queue<Float> cuWorkload = resourceWorkloadMap.get(RuleSupport.ResourceType.CPU.name());

      long startedTime = System.currentTimeMillis();
      log.info("=========== monitoring of group: " + groupId + " started at: " + startedTime);

      final Timer timer = new Timer();
      timer.scheduleAtFixedRate(new TimerTask() {

        @Override
        public void run() {
          if (isMonitoringActivated) {
            ResourceMonitoringEvent resourceMonitoringEvent;

            float ramRequirement = ramWorkload.poll();
           /* int noOfGBsInSys = 4; //TODO-get this from Karamel API
            if (ramRequirement > 4 )
              noOfGBsInSys = 8;    //TODO dynamically update with Karamel API*/
            float noOfGBsInSys = getTotalRamInGroup();

            float ramUtilization = ramRequirement/noOfGBsInSys * 100; //TODO get this for each machine to send utilization events

            Float highRamThreshold = greaterThanInterestMap.get(RuleSupport.ResourceType.RAM.name());
            Float lowRamThreshold = lessThanInterestMap.get(RuleSupport.ResourceType.RAM.name());

            float cuRequirement = cuWorkload.poll();
            ////int noOfCUsInSys = 2; //TODO-get this from Karamel API
            int noOfCUsInSys = getTotalCusInGroup();

            float cpuUtilization = cuRequirement/noOfCUsInSys * 100; //TODO get this for each machine to send utilization events

            Float highCpuThreshold = greaterThanInterestMap.get(RuleSupport.ResourceType.CPU.name());
            Float lowCpuThreshold = lessThanInterestMap.get(RuleSupport.ResourceType.CPU.name());

            for (String resThresholdsString : durationGreaterInterestMap.values()) {
              String[] items = resThresholdsString.split(":"); //ie: CPU:70:80
              if (items.length == 3) {
                if (RuleSupport.ResourceType.RAM.name().equals(items[0])) {
                  //I am just assigning the lower value here since we always add timed events a delta less than original interest
                  highRamThreshold = Float.valueOf(items[1]);
                } else if (RuleSupport.ResourceType.CPU.name().equals(items[0])) {
                  //I am just assigning the lower value here since we always add timed events a delta less than original interest
                  highCpuThreshold = Float.valueOf(items[1]);
                }
              }
            }

            for (String resThresholdsString : durationLessInterestMap.values()) {
              String[] items = resThresholdsString.split(":"); //ie: CPU:20:30
              if (items.length == 3) {
                if (RuleSupport.ResourceType.RAM.name().equals(items[0])) {
                  //I am just assigning the lower value here since we always add timed events a delta less than original interest
                  lowRamThreshold = Float.valueOf(items[2]);
                } else if (RuleSupport.ResourceType.CPU.name().equals(items[0])) {
                  //I am just assigning the lower value here since we always add timed events a delta less than original interest
                  lowCpuThreshold = Float.valueOf(items[2]);
                }
              }
            }

          /*  log.info("++++++++++++ new RAM interests, high:low ++++++++++++" + highRamThreshold + ":" + lowRamThreshold);
            log.info("++++++++++++ new CPU interests, high:low ++++++++++++" + highCpuThreshold + ":" + lowCpuThreshold);*/

            boolean eventSent = false;
            if (highRamThreshold != null && ramUtilization >= highRamThreshold) {
              //TODO get machine Ids and send values for all machine Ids
              resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.RAM,
                      RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, ramUtilization);
              try {
                log.info("....... going to add onHighRam event. Utilization: " + resourceMonitoringEvent.getCurrentValue());
                monitoringListener.onHighRam(groupId, resourceMonitoringEvent);
                eventSent = true;
              } catch (HoneyTapException e) {
                log.error("Error while sending onHighRam event for groupId: " + groupId + " machine: ", e);
              }
            } else if (lowRamThreshold != null && ramUtilization <= lowRamThreshold) {
              resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.RAM,
                      RuleSupport.Comparator.LESS_THAN_OR_EQUAL, ramUtilization);
              try {
                log.info("....... going to add onLowRam event. Utilization: " + resourceMonitoringEvent.getCurrentValue());
                monitoringListener.onLowRam(groupId, resourceMonitoringEvent);
                eventSent = true;
              } catch (HoneyTapException e) {
                log.error("Error while sending onLowRam event for groupId: " + groupId + " machine: ", e);
              }
            }

            if (highCpuThreshold != null && cpuUtilization >= highCpuThreshold) {
              resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.CPU,
                      RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, cpuUtilization);
              try {
                log.info("....... going to add onHighCPU event. Utilization: " + resourceMonitoringEvent.getCurrentValue());
                monitoringListener.onHighCPU(groupId, resourceMonitoringEvent);
                eventSent = true;
              } catch (HoneyTapException e) {
                log.error("Error while sending onHighCPU event for groupId: " + groupId + " machine: ", e);
              }
            } else if (lowCpuThreshold != null && cpuUtilization <= lowCpuThreshold) {
              resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.CPU,
                      RuleSupport.Comparator.LESS_THAN_OR_EQUAL, cpuUtilization);
              try {
                log.info("....... going to add onHighRam event. Utilization: " + resourceMonitoringEvent.getCurrentValue());

                monitoringListener.onLowCPU(groupId, resourceMonitoringEvent);
                eventSent = true;
              } catch (HoneyTapException e) {
                log.error("Error while sending onLowCPU event for groupId: " + groupId + " machine: ", e);
              }
            }
            if (!eventSent) {
              log.info(".................. no events occured .......... Cpu, Ram ........." + cpuUtilization + "," +
                      ramUtilization);
            }
          }
        }
      }, 0, monitoringFreqSeconds * 1000);
    }

    private float getTotalRamInGroup() {
      float gbs = 0.0f;

      //fill machine runtime info
      for (VM vmInfo : vmMap.values()) {
        gbs += vmInfo.memInGig;
      }
      if (gbs == 0.0f) {
        gbs = initialSystemRam;
      }

      //fill simulated machine info
      for (VM vmInfo : simulatedVmMap.values()) {
        gbs += vmInfo.memInGig;
      }

      return gbs;
    }

    private int getTotalCusInGroup() {
      int cus = 0;

      //fill machine runtime info
      for (VM vmInfo : vmMap.values()) {
        cus += vmInfo.numVCpu;
      }
      if (cus == 0) {
        cus = initialSystemCus;
      }

      //fill simulated machine info
      for (VM vmInfo : simulatedVmMap.values()) {
        cus += vmInfo.numVCpu;
      }

      return cus;
    }

    public void addDurationLessInterest(String id, String resourceThreshold) {
      durationGreaterLock.lock();
      this.durationLessInterestMap.put(id, resourceThreshold);
      durationGreaterLock.unlock();
    }

    public void removeDurationLessInterest(String id) {
      durationGreaterLock.lock();
      this.durationLessInterestMap.remove(id);
      durationGreaterLock.unlock();
    }

    public void addDurationGreaterInterest(String id, String resourceThreshold) {
      durationLessLock.lock();
      this.durationGreaterInterestMap.put(id, resourceThreshold);
      durationLessLock.unlock();
    }

    public void removeDurationGreaterInterest(String id) {
      durationLessLock.lock();
      this.durationGreaterInterestMap.remove(id);
      durationLessLock.unlock();
    }

    public void addVMInfo(String vmId, int numVCpu, double memInGig, Integer numDisks, Integer diskSize, boolean reset) {
      if (reset) {
        vmMap = new HashMap<>();
      }
      VM vm = new VM(vmId, numVCpu, memInGig, numDisks, diskSize);
      vmMap.put(vmId, vm);
      log.info("====== resetting a real VM. System status ram, cus : " + getTotalRamInGroup() + ", " + getTotalCusInGroup());
    }

    public void addSimulatedVMInfo(String vmId, int numVCpu, double memInGig, Integer numDisks, Integer diskSize) {
      VM vm = new VM(vmId, numVCpu, memInGig, numDisks, diskSize);
      simulatedVmMap.put(vmId, vm);
      log.info("====== adding a simulated VM. System status ram, cus : " + getTotalRamInGroup() + ", " + getTotalCusInGroup());
    }

    public void removeSimulatedVMInfo(String vmId) {
        simulatedVmMap.remove(vmId);
        log.info("====== removing a simulated VM. System status ram, cus : " + getTotalRamInGroup() + ", " + getTotalCusInGroup());
    }

    public String[] getAllSimulatedVmIds() {
      ArrayList<String> simulatedVmIds = new ArrayList<>();
      for (String vmId : simulatedVmMap.keySet()) {
        simulatedVmIds.add(vmId);
      }
      return simulatedVmIds.toArray(new String[simulatedVmIds.size()]);
    }

    private class VM {
      String vmId;
      int numVCpu;
      double memInGig;
      Integer numDisks;
      Integer diskSize;

      public VM(String vmId, int numVCpu, double memInGig, Integer numDisks, Integer diskSize) {
        this.vmId = vmId;
        this.numVCpu = numVCpu;
        this.memInGig = memInGig;
        this.numDisks = numDisks;
        this.diskSize = diskSize;
      }
    }
  }

  class TimedEventHandler implements Runnable{

    private String groupId;
    private InterestedEvent[] events;
    private int timeDurationSec;
    private EventProducer producer;

    public TimedEventHandler(String groupId, InterestedEvent[] events, int timeDurationSec, EventProducer producer) {
      this.groupId = groupId;
      this.events = events;
      this.timeDurationSec = timeDurationSec;
      this.producer = producer;
    }

    @Override
    public void run() {
      ArrayList<String> greaterIds = new ArrayList<>();
      ArrayList<String> lessIds = new ArrayList<>();

      for (InterestedEvent event : events) {
        String id;
        //resource interests: CPU:AVG:>=:10:<=:90  ; CPU:>=:70:<=:80
        String items[] = event.getInterest().split(Constants.SEPARATOR);

        if (items.length == 5) {  //ie: CPU:>=:70:<=:80
          if (RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.equals(RuleSupport.getNormalizedComparatorType(event.getComparator()))) {

            id = UUID.randomUUID().toString();
            greaterIds.add(id);
            String resourceThreshold = event.getInterest().replace("GREATER_THAN_OR_EQUAL:","").replace("LESS_THAN_OR_EQUAL:","");
            producer.addDurationGreaterInterest(id, resourceThreshold);    //ID: <ResourceType:thresholdLow:thresholdHigh> map
          } else if (RuleSupport.Comparator.LESS_THAN_OR_EQUAL.equals(RuleSupport.getNormalizedComparatorType(event.getComparator()))) {
            id = UUID.randomUUID().toString();
            lessIds.add(id);
            String resourceThreshold = event.getInterest().replace("GREATER_THAN_OR_EQUAL:","").replace("LESS_THAN_OR_EQUAL:","");
            producer.addDurationLessInterest(id, resourceThreshold);    //ID: <ResourceType:thresholdLow:thresholdHigh> map
          }
        } else if (event.getInterest().contains(Constants.AVERAGE) && items.length == 6) {  //ie: CPU:AVG:>=:10:<=:90
          //TODO implement
        }
      }
      try {
        Thread.sleep(timeDurationSec * 1000);
      } catch (InterruptedException e) {
        log.error("Error while waiting until duration is over for duration based interest events for group: " +
                groupId + "Have to remove the duration based interests now :(");
      } finally {
        for (String id : greaterIds) {
          producer.removeDurationGreaterInterest(id);
        }
        for (String id : lessIds) {
          producer.removeDurationLessInterest(id);
        }
      }
    }
  }
}
