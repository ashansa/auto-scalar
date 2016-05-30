package se.kth.autoscalar.scaling.monitoring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.scaling.Constants;
import se.kth.autoscalar.scaling.core.AutoScalarAPI;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MonitoringHandlerSimulator implements MonitoringHandler{

  Log log = LogFactory.getLog(MonitoringHandlerSimulator.class);
  private AutoScalarAPI autoScalarAPI;
  MonitoringListener monitoringListener;
  Map<String, EventProducer> producerMap = new HashMap<>();


  public MonitoringHandlerSimulator(AutoScalarAPI autoScalarAPI) {
    this.autoScalarAPI = autoScalarAPI;
    this.monitoringListener = new MonitoringListener(this.autoScalarAPI);
  }

  public MonitoringListener addGroupForMonitoring(String groupId, InterestedEvent[] interestedEvents) throws AutoScalarException {
    //TODO stimulator will consider interested events only with = sign (no lessThan, greaterThan for simulation)
    //lessThan =====will be changed as ====> lessThanOrEqual
    String cpuWorkload = "20:50";
    //String ramWorkload = "1:10, 5:50, 10:95, 5:57, 5:180, 10:62, 4:12";
    String ramWorkload = "5:10, 2:50, 7:95, 6:12";
    EventProducer eventProducer = new EventProducer(groupId, monitoringListener, cpuWorkload, ramWorkload, 1);
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

  public void addInterestedEvent(String groupId, InterestedEvent[] events, int timeDuration) {
    System.out.println("============= addInterestedEvent with duration not yet implemented ============");

    //TODO request events for a limited time
    //should handle all interested event types
    //resource interests: CPU:>=:80; RAM:<=:30;   CPU:AVG:>=:10:<=:90  ; CPU:>=:70:<=:80
    // machine interests: KILLED; AT_END_OF_BILLING_PERIOD

    /*for (InterestedEvent event : events) {
      try {
        String items[] = event.getInterest().split(Constants.SEPARATOR);
        if (event.getInterest().contains(Constants.AVERAGE) && items.length == 6) {  //ie: CPU:AVG:>=:10:<=:90
          tablespoonAPI.createTopic(monitoringListener, groupId, EventType.GROUP_AVERAGE, MonitoringUtil.
                  getMonitoringResourceType(items[0]), timeDuration, MonitoringUtil.
                  getMonitoringThreshold(items[4], items[5]), MonitoringUtil.getMonitoringThreshold(
                  items[2], items[3]));
        } else if (items.length == 5) {  //ie: CPU:>=:70:<=:80
          tablespoonAPI.createTopic(monitoringListener, groupId, EventType.REGULAR, MonitoringUtil.
                  getMonitoringResourceType(items[0]), timeDuration, MonitoringUtil.getMonitoringThreshold(
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

  class EventProducer {

    boolean isMonitoringActivated;
    String groupId;
    MonitoringListener monitoringListener;
    int monitoringFreq;   //each 1sec, 2sec, etc
    Map<String,Queue<Float>> resourceWorkloadMap = new HashMap<>();   //CPU: 12,12,12,12,52,52,.... (monitoring values)

    Map<String, Float> greaterThanRuleMap = new HashMap<>();
    Map<String, Float> lessThanRuleMap = new HashMap<>();
    //TODO if we need to handle >1 group, need to have a map of groupId-greater/lessThanRuleMap
    //workload

    //spawning delay
    //monitoring events delay??? (AS can't help if monitoring is delayed)

    EventProducer(String groupId, MonitoringListener monitoringListener, String cpuWorkload, String ramWorkload, int monitoringFreq) {
      this.groupId = groupId;
      this.monitoringListener = monitoringListener;
      this.monitoringFreq = monitoringFreq;

      //minutes:resource_utilization%
      addWorkload(RuleSupport.ResourceType.CPU.name(), cpuWorkload);
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
        for (int i = 0; i < durationMin * 60; i = i + monitoringFreq) {
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
            throw new Exception("Interested event is not in the correct format :" + interestedEvent.getInterest());
          }
          RuleSupport.Comparator unifiedComparator = RuleSupport.getNormalizedComparatorType(
                  RuleSupport.Comparator.valueOf(items[1]));

          Float definedThreshold = Float.valueOf(items[2]);

          if (RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.equals(unifiedComparator)) {
            Float existingValue = greaterThanRuleMap.get(items[1]);
            if (existingValue == null) {
              greaterThanRuleMap.put(items[1], definedThreshold);
            } else {
              //since greater than, go for the lowest threshold
              if (definedThreshold < existingValue) {
                greaterThanRuleMap.put(items[1], definedThreshold);
              } //else no need to change since new value includes in existing threshold
            }
          } else { //this is lessThanOrEqual part
            Float existingValue = lessThanRuleMap.get(items[1]);
            if (existingValue == null) {
              lessThanRuleMap.put(items[1], definedThreshold);
            } else {
              //since less than, go for the highest threshold
              if (definedThreshold > existingValue) {
                lessThanRuleMap.put(items[1], definedThreshold);
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
      System.out.println("========= going to stop monitoring of group: " + groupId);
      isMonitoringActivated = false;
    }

    public void startMonitoring() {
      //send events periodically considering the workload and the interested events
      //ie RAM workload : "1:10, 5:50, 10:95, 5:57, 5:180, 10:62, 5:12"  min:utilization
      final Queue<Float> ramMonitoring = resourceWorkloadMap.get(RuleSupport.ResourceType.RAM.name());
      final Queue<Float> cpuMonitoring = resourceWorkloadMap.get(RuleSupport.ResourceType.CPU.name());

      long startedTime = System.currentTimeMillis();
      System.out.println("=========== monitoring of group: " + groupId + " started at: " + startedTime);

      final Timer timer = new Timer();
      timer.scheduleAtFixedRate(new TimerTask() {

        @Override
        public void run() {
          if (isMonitoringActivated) {
            ResourceMonitoringEvent resourceMonitoringEvent;

            float ramUtilization = ramMonitoring.poll();
            Float highRamThreshold = greaterThanRuleMap.get(RuleSupport.ResourceType.RAM.name());
            Float lowRamThreshold = lessThanRuleMap.get(RuleSupport.ResourceType.RAM.name());

            if (highRamThreshold != null && ramUtilization >= highRamThreshold) {
              //TODO get machine Ids and send values for all machine Ids
              resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.RAM,
                      RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, ramUtilization);
              try {
                monitoringListener.onHighRam(groupId, resourceMonitoringEvent);
              } catch (AutoScalarException e) {
                log.error("Error while sending onHighRam event for groupId: " + groupId + " machine: ", e);
              }
            } else if (lowRamThreshold != null && ramUtilization <= lowRamThreshold) {
              resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.RAM,
                      RuleSupport.Comparator.LESS_THAN_OR_EQUAL, ramUtilization);
              try {
                monitoringListener.onLowRam(groupId, resourceMonitoringEvent);
              } catch (AutoScalarException e) {
                log.error("Error while sending onLowRam event for groupId: " + groupId + " machine: ", e);
              }
            }

            float cpuUtilization = cpuMonitoring.poll();
            Float highCpuThreshold = greaterThanRuleMap.get(RuleSupport.ResourceType.CPU.name());
            Float lowCpuThreshold = lessThanRuleMap.get(RuleSupport.ResourceType.CPU.name());
            if (highCpuThreshold != null && cpuUtilization >= highCpuThreshold) {
              resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.CPU,
                      RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, cpuUtilization);
              try {
                monitoringListener.onHighCPU(groupId, resourceMonitoringEvent);
              } catch (AutoScalarException e) {
                log.error("Error while sending onHighCPU event for groupId: " + groupId + " machine: ", e);
              }
            } else if (lowCpuThreshold != null && cpuUtilization <= lowCpuThreshold) {
              resourceMonitoringEvent = new ResourceMonitoringEvent(groupId,"??machineId", RuleSupport.ResourceType.CPU,
                      RuleSupport.Comparator.LESS_THAN_OR_EQUAL, cpuUtilization);
              try {
                monitoringListener.onLowCPU(groupId, resourceMonitoringEvent);
              } catch (AutoScalarException e) {
                log.error("Error while sending onLowCPU event for groupId: " + groupId + " machine: ", e);
              }
            }
          }
        }
      }, 0, monitoringFreq);
    }

   /* @Override
    public void run() {
      //send events periodically considering the workload and the interested events
      //ie RAM workload : "1:10, 5:50, 10:95, 5:57, 5:180, 10:62, 5:12"  min:utilization
      Queue<WLTuple> cpuMonitoring = resourceWorkloadMap.get(RuleSupport.ResourceType.CPU.name());
      Queue<WLTuple> ramMonitoring = resourceWorkloadMap.get(RuleSupport.ResourceType.RAM.name());

      long startedTime = System.currentTimeMillis();
      System.out.println("=========== monitoring of group: " + groupId + " started at: " + startedTime);
      while (isMonitoringActivated) {

      }

      while (isMonitoringActivated) {
        if (ramUtilizeLevels.length >= cpuUtilizeLevels.length) {
          for (String ramUtilizeLevel : ramUtilizeLevels) {
            Float timeInMin = Float.valueOf(ramUtilizeLevel.trim().split(":")[0]);
            Float utilization = Float.valueOf(ramUtilizeLevel.trim().split(":")[1]);

            long timeToExit =  System.currentTimeMillis() + (long)(timeInMin * 60 * 1000);
            while (System.currentTimeMillis() <= timeToExit) {
               if (utilization >= greaterThanRuleMap.get(RuleSupport.ResourceType.RAM.name())) {
                 monitoringListener.onHighRam(groupId, );
               } else if (utilization <= lessThanRuleMap.get(RuleSupport.ResourceType.RAM.name())) {
                 monitoringListener.onLowRam(groupId, );
               }
            }
          }
        }
      }
    }*/
  }
}
