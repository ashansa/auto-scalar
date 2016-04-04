package se.kth.autoscalar.scaling.profile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.scaling.monitoring.MachineMonitoringEvent;
import se.kth.autoscalar.scaling.monitoring.MonitoringEvent;
import se.kth.autoscalar.scaling.monitoring.ResourceMonitoringEvent;
import se.kth.autoscalar.scaling.core.AutoScalingManager;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//import se.kth.autoscalar.scaling.core.AutoScalingManager;

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
                eventsToBeProfiled = processMachineEvents(eventsToBeProfiled);
                ArrayList<MonitoringEvent> events;

                //process remaining resource events
                for (String eventKey : eventsToBeProfiled.keySet()) {
                    events = eventsToBeProfiled.get(eventKey); //this is only one type of events in a group
                    //since the machine monitoring events are already processed, only the resource events will be remaining to be processed
                    if (eventKey.endsWith(RESOURCE_EVENT)) {
                        for (MonitoringEvent monitoringEvent : events) {
                            //as first step, just adding every event to profiled events queue
                            //TODO call getProfiledResourceEvent when its logic is implemented. May have to update the tests
                            ResourceMonitoringEvent event = (ResourceMonitoringEvent) monitoringEvent;
                            ProfiledEvent profiledEvent = new ProfiledResourceEvent(getGroupId(eventKey), event.getResourceType(),
                                    event.getComparator(), event.getCurrentValue());
                            notifyListeners(profiledEvent);
                        }
                    }
                }

                try {
                    lock.lock();
                    //after processing is done, add events added to temp map to eventsToBeProcessed so that they will
                    // be processed in the next scheduling round
                    eventsToBeProfiled = eventsToBeProfiledTempMap;
                    eventsToBeProfiledTempMap = new HashMap<String, ArrayList<MonitoringEvent>>();
                    isProcessingInProgress = false;
                } finally {
                    lock.unlock();
                }
            }
        }, 10, 20);
    }

    private Map<String, ArrayList<MonitoringEvent>> processMachineEvents(Map<String, ArrayList<MonitoringEvent>> eventsToBeProfiled) {
        Set<String> keys = new HashSet<String>();
        keys.addAll(eventsToBeProfiled.keySet());
        for (String eventKey : keys) {
            if (eventKey.endsWith(MACHINE_EVENT)) {
                ArrayList<MonitoringEvent> machineEventsInGroup = eventsToBeProfiled.get(eventKey);

                String groupId = getGroupId(eventKey);
                String resourceEventKeyOfGroup = getProfiledEventKey(groupId, ResourceMonitoringEvent.class);
                ArrayList<MonitoringEvent> resourceEventsInGroup = eventsToBeProfiled.get(resourceEventKeyOfGroup);
                ProfiledResourceEvent profiledResourceEvent = null;

                if (resourceEventsInGroup != null) {
                    profiledResourceEvent = getProfiledResourceEvent(groupId, resourceEventsInGroup.toArray(new ResourceMonitoringEvent[resourceEventsInGroup.size()]));
                    eventsToBeProfiled.remove(resourceEventKeyOfGroup);
                }

                ProfiledMachineEvent profiledMachineEvent = new ProfiledMachineEvent(groupId, machineEventsInGroup.toArray(
                        new MachineMonitoringEvent[machineEventsInGroup.size()]), profiledResourceEvent);
                notifyListeners(profiledMachineEvent);
                eventsToBeProfiled.remove(eventKey);
            }
        }
        return eventsToBeProfiled;
    }

    private ProfiledResourceEvent getProfiledResourceEvent(String groupId, ResourceMonitoringEvent[] resourceEvents) {
        //TODO consider all resource events and create the profiled resource event. (CAN use same method as if(eventKey.endsWith(RESOURCE_EVENT))
        ResourceMonitoringEvent resE = (ResourceMonitoringEvent) resourceEvents[0];
        ProfiledResourceEvent profiledResourceEvent = new ProfiledResourceEvent(groupId, resE.getResourceType(), resE.getComparator(), resE.getCurrentValue());
        return profiledResourceEvent;
    }

    private void notifyListeners(ProfiledEvent profiledEvent) {

        for (ProfiledEventListener listener : profiledEventListeners) {
            try {
                if (profiledEvent instanceof ProfiledResourceEvent && listener instanceof AutoScalingManager.ProfiledResourceEventListener) {
                    AutoScalingManager.ProfiledResourceEventListener resourceEventListener = (AutoScalingManager.ProfiledResourceEventListener) listener;
                    ProfiledResourceEvent resourceEvent = (ProfiledResourceEvent) profiledEvent;
                    resourceEventListener.handleEvent(resourceEvent);
                } else if (profiledEvent instanceof ProfiledMachineEvent && listener instanceof AutoScalingManager.ProfiledMachineEventListener) {
                    AutoScalingManager.ProfiledMachineEventListener machineEventListener = (AutoScalingManager.ProfiledMachineEventListener) listener;
                    ProfiledMachineEvent machineEvent = (ProfiledMachineEvent) profiledEvent;
                    machineEventListener.handleEvent(machineEvent);
                }

            } catch(AutoScalarException e){
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
