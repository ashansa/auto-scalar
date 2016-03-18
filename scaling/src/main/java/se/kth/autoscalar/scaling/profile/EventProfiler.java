package se.kth.autoscalar.scaling.profile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.common.monitoring.MachineMonitoringEvent;
import se.kth.autoscalar.common.monitoring.MonitoringEvent;
import se.kth.autoscalar.common.monitoring.ResourceMonitoringEvent;
import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.core.ElasticScalingManager;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//import se.kth.autoscalar.scaling.core.ElasticScalingManager;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class EventProfiler {

    Log log = LogFactory.getLog(EventProfiler.class);

    //<groupId:eventType> <MonitoringEvent ArrayList> maps    //ie of key: group_8:resourceEvent
    private Map<String, ArrayList<MonitoringEvent>> eventsToBeProfiled = new HashMap<String, ArrayList<MonitoringEvent>>();
    private Map<String, ArrayList<MonitoringEvent>> eventsToBeProfiledTempMap = new HashMap<String, ArrayList<MonitoringEvent>>();

    ArrayList<ProfiledEventListener> profiledEventListeners = new ArrayList<ProfiledEventListener>();

    boolean isProcessingInProgress;
    private Lock lock = new ReentrantLock();

    private static final String RESOURCE_EVENT = "resourceEvent";
    private static final String MACHINE_EVENT = "machineEvent";

    public EventProfiler() {
        init();
    }

    public void addListener(ProfiledEventListener listener) {
        profiledEventListeners.add(listener);
    }

    private void init() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                isProcessingInProgress = true;
                ArrayList<MonitoringEvent> eventsToProfileInGroup;
                for (String profiledEventKey : eventsToBeProfiled.keySet()) {
                    try {
                        eventsToProfileInGroup = eventsToBeProfiled.get(profiledEventKey); //this is only one type of events in a group
                        //since key concatenates event type too
                        if (profiledEventKey.endsWith(RESOURCE_EVENT)) {
                            for (MonitoringEvent monitoringEvent : eventsToProfileInGroup) {
                                //as first step, just adding every event to profiled events queue
                                //TODO do profiling and add the result
                                ResourceMonitoringEvent event = (ResourceMonitoringEvent) monitoringEvent;
                                ProfiledEvent profiledEvent = new ProfiledResourceEvent(getGroupId(profiledEventKey), event.getResourceType(),
                                        event.getComparator(), event.getCurrentValue());
                                notifyListeners(profiledEvent);
                            }
                        } else if (profiledEventKey.endsWith(MACHINE_EVENT)) {

                            boolean isUnderUtilized = false;
                            ProfiledEvent profiledEvent = null;
                            for (MonitoringEvent monitoringEvent : eventsToProfileInGroup) {
                                MachineMonitoringEvent event = (MachineMonitoringEvent) monitoringEvent;

                                if (MachineMonitoringEvent.Status.AT_END_OF_BILLING_PERIOD.name().equals(
                                        event.getStatus().name())) {

                                    //Profiling machines which are closer to end of billing period will be terminated
                                    //if the resources are underutilized according to client specified resource requirements
                                    String groupId = getGroupId(profiledEventKey);
                                    ArrayList<MonitoringEvent> resourceMonitoringEventsOfGroup = eventsToBeProfiled.
                                            get(getProfiledEventKey(groupId, ResourceMonitoringEvent.class));


                                    //TODO profile and decide whether to kill the machine or not
                                    for (MonitoringEvent resourceEvent : resourceMonitoringEventsOfGroup) {
                                        String eventComparator = ((ResourceMonitoringEvent) resourceEvent).getComparator().name();
                                        if (RuleSupport.Comparator.LESS_THAN.name().equals(eventComparator) ||
                                                RuleSupport.Comparator.LESS_THAN_OR_EQUAL.name().equals(eventComparator)) {
                                            profiledEvent = new ProfiledMachineEvent(event.getMachineId());
                                            isUnderUtilized = true;  //TODO: don't decide just by using one event in outer for loop
                                            return;
                                        }
                                    }

                                    if (isUnderUtilized == true && profiledEvent != null) {
                                        notifyListeners(profiledEvent);
                                    }

                                } else if (MachineMonitoringEvent.Status.KILLED.name().equals(event.getStatus().name())) {
                                    //support later
                                }

                            }
                        }
                    } catch (ClassCastException e) {
                        log.error("Error while casting the event to specific type. ProfiledEventKey: " +
                                profiledEventKey + " . " + e.getMessage());
                    }
                }
                try {
                    lock.lock();
                    //after processing is done, add events added to temp map to eventsToBeProcessed so that they will
                    // be processed in the next scheduling round
                    eventsToBeProfiled = eventsToBeProfiledTempMap;
                    eventsToBeProfiledTempMap = new HashMap<String, ArrayList<MonitoringEvent>>();

                } finally {
                    lock.unlock();
                }
            }
        }, 1000, 1000);
    }

    private void notifyListeners(ProfiledEvent profiledEvent) {

        for (ProfiledEventListener listener : profiledEventListeners) {
            try {
                if (profiledEvent instanceof ProfiledResourceEvent && listener instanceof ElasticScalingManager.ProfiledResourceEventListener) {
                    ElasticScalingManager.ProfiledResourceEventListener resourceEventListener = (ElasticScalingManager.ProfiledResourceEventListener) listener;
                    ProfiledResourceEvent resourceEvent = (ProfiledResourceEvent) profiledEvent;
                    resourceEventListener.handleEvent(resourceEvent);
                } else if (profiledEvent instanceof ProfiledMachineEvent && listener instanceof ElasticScalingManager.ProfiledMachineEventListener) {
                    ElasticScalingManager.ProfiledMachineEventListener machineEventListener = (ElasticScalingManager.ProfiledMachineEventListener) listener;
                    ProfiledMachineEvent machineEvent = (ProfiledMachineEvent) profiledEvent;
                    machineEventListener.handleEvent(machineEvent);
                }

            } catch(ElasticScalarException e){
                throw new IllegalStateException(e);
            }
        }
    }

    public void profileEvent(String groupId, MonitoringEvent monitoringEvent) {
        String profiledEventKey = getProfiledEventKey(groupId, monitoringEvent.getClass());

        if(isProcessingInProgress) {
            lock.lock();
            //adding events to temp map since the event map is processing (eventsToBeProfiledTempMap)

            ArrayList<MonitoringEvent> events;
            if (eventsToBeProfiledTempMap.containsKey(profiledEventKey)) {
                events = eventsToBeProfiledTempMap.get(profiledEventKey);
            } else {
                events = new ArrayList<MonitoringEvent>();
            }

            events.add(monitoringEvent);
            eventsToBeProfiledTempMap.put(profiledEventKey, events);

            lock.unlock();
        } else {
            // add to eventsToBeProfiled map
            ArrayList<MonitoringEvent> events;
            if (eventsToBeProfiled.containsKey(profiledEventKey)) {
                events = eventsToBeProfiled.get(profiledEventKey);
            } else {
                events = new ArrayList<MonitoringEvent>();
            }

            events.add(monitoringEvent);
            eventsToBeProfiled.put(profiledEventKey, events);
        }

    }

    private String getProfiledEventKey(String groupId, Class eventClass) {
        if (eventClass == ResourceMonitoringEvent.class)
            return groupId.concat(":").concat(RESOURCE_EVENT);
        else if (eventClass == MachineMonitoringEvent.class)
            return groupId.concat(":").concat(MACHINE_EVENT);
        else
            return groupId;
    }

    private String getGroupId(String profiledEventKey) {
        return profiledEventKey.substring(0, profiledEventKey.lastIndexOf(":"));
    }
}
