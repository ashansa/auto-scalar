package se.kth.autoscalar.scaling.profile;

import se.kth.autoscalar.common.monitoring.MachineMonitoringEvent;
import se.kth.autoscalar.common.monitoring.MonitoringEvent;
import se.kth.autoscalar.common.monitoring.ResourceMonitoringEvent;
import se.kth.autoscalar.scaling.core.ElasticScalingManager;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class EventProfiler {

    //<groupId> <MonitoringEvent ArrayList> maps
    private Map<String, ArrayList<MonitoringEvent>> eventsToBeProfiled = new HashMap<String, ArrayList<MonitoringEvent>>();
    private Map<String, ArrayList<MonitoringEvent>> eventsToBeProfiledTempMap = new HashMap<String, ArrayList<MonitoringEvent>>();

    ArrayList<ProfiledEventListener> profiledEventListeners = new ArrayList<ProfiledEventListener>();

    boolean isProcessingInProgress;
    private Lock lock = new ReentrantLock();

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
                for (String groupId : eventsToBeProfiled.keySet()) {
                    eventsToProfileInGroup = eventsToBeProfiled.get(groupId);
                   //TODO do profiling
                    for (MonitoringEvent monitoringEvent : eventsToProfileInGroup) {
                        if (monitoringEvent instanceof ResourceMonitoringEvent) {
                            //as first step, just adding every event to profiled events queue
                            //TODO do profiling and add the result
                            ResourceMonitoringEvent event = (ResourceMonitoringEvent)monitoringEvent;
                            ProfiledEvent profiledEvent = new ProfiledResourceEvent(groupId, event.getResourceType(),
                                    event.getComparator(), event.getCurrentValue());
                            notifyListeners(profiledEvent);
                        } else if (monitoringEvent instanceof MachineMonitoringEvent) {
                            //support later
                        }
                    }
                }
                try {
                    lock.lock();
                    //after processing is done, add events added to temp map to eventsToBeProcessed so that they will
                    // be processed in the next sheduling round
                    eventsToBeProfiled = eventsToBeProfiledTempMap;
                    eventsToBeProfiledTempMap = new HashMap<String, ArrayList<MonitoringEvent>>();

                } finally {
                    lock.unlock();
                }
            }
        }, 1000, 10000);
    }

    private void notifyListeners(ProfiledEvent profiledEvent) {

        for (ProfiledEventListener listener : profiledEventListeners) {
            try {
                if (profiledEvent instanceof ProfiledResourceEvent && listener instanceof ElasticScalingManager.ProfiledResourceEventListener) {
                    ElasticScalingManager.ProfiledResourceEventListener resourceEventListener = (ElasticScalingManager.ProfiledResourceEventListener) listener;
                    ProfiledResourceEvent resourceEvent = (ProfiledResourceEvent) profiledEvent;
                    resourceEventListener.handleEvent(resourceEvent);
                }
                //TODO else if part , if a machine monitoring event
            } catch(ElasticScalarException e){
                throw new IllegalStateException(e);
            }
        }
    }

    public void profileEvent(String groupId, MonitoringEvent monitoringEvent) {
        if(isProcessingInProgress) {
            lock.lock();
            //adding events to temp map since the event map is processing (eventsToBeProfiledTempMap)

            ArrayList<MonitoringEvent> events;
            if (eventsToBeProfiledTempMap.containsKey(groupId)) {
                events = eventsToBeProfiledTempMap.get(groupId);
            } else {
                events = new ArrayList<MonitoringEvent>();
            }

            events.add(monitoringEvent);
            eventsToBeProfiledTempMap.put(groupId, events);

            lock.unlock();
        } else {
            // add to eventsToBeProfiled map
            ArrayList<MonitoringEvent> events;
            if (eventsToBeProfiled.containsKey(groupId)) {
                events = eventsToBeProfiled.get(groupId);
            } else {
                events = new ArrayList<MonitoringEvent>();
            }

            events.add(monitoringEvent);
            eventsToBeProfiled.put(groupId, events);
        }

    }
}
