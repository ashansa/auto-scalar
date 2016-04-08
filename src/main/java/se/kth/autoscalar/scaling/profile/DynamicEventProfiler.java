package se.kth.autoscalar.scaling.profile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.scaling.core.AutoScalingManager;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;
import se.kth.autoscalar.scaling.monitoring.InterestedEvent;
import se.kth.autoscalar.scaling.monitoring.MonitoringEvent;
import se.kth.autoscalar.scaling.monitoring.MonitoringHandler;

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
public class DynamicEventProfiler {

    Log log = LogFactory.getLog(DynamicEventProfiler.class);

    //<groupId> <MonitoringEvent ArrayList> maps    //ie of key: group_8
    private Map<String, ArrayList<MonitoringEvent>> eventsToBeProfiled = new HashMap<String, ArrayList<MonitoringEvent>>();
    private Map<String, ArrayList<MonitoringEvent>> eventsToBeProfiledTempMap = new HashMap<String, ArrayList<MonitoringEvent>>();

    private ArrayList<ProfiledEventListener> profiledEventListeners = new ArrayList<ProfiledEventListener>();
    private MonitoringHandler monitoringHandler;

    //TODO make this configurable
    private int windowSize = 20; //time in ms
    private int timeOutForNewEvents = windowSize/3;
    private boolean isProcessingInProgress;
    private Lock lock = new ReentrantLock();

    public DynamicEventProfiler(MonitoringHandler monitoringHandler) {
        this.monitoringHandler = monitoringHandler;
        init();
    }

    public void addListener(ProfiledEventListener listener) {
        profiledEventListeners.add(listener);
    }

    public void init() {
        final Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                isProcessingInProgress = true;
                ArrayList<MonitoringEvent> eventsInGroup;

                for (String groupId : eventsToBeProfiled.keySet()) {
                    eventsInGroup = eventsToBeProfiled.get(groupId);

                    ArrayList<InterestedEvent> newInterests = new ArrayList<InterestedEvent>();
                    //TODO analyze events and add new Interests to array
                    monitoringHandler.addInterestedEvent(groupId, newInterests.toArray(
                            new InterestedEvent[newInterests.size()]), timeOutForNewEvents);    //TODO work on partitionSize

                    try {
                        Thread.sleep(timeOutForNewEvents);
                    } catch (InterruptedException e) {
                        log.warn("Thread sleep interrupted while waiting for new interested events, part 1");
                    }
                    ArrayList<MonitoringEvent> newEvents;
                    try {
                        lock.lock();
                        newEvents = eventsToBeProfiledTempMap.get(groupId);
                        if(newEvents != null) {
                            eventsToBeProfiledTempMap.put(groupId, new ArrayList<MonitoringEvent>());
                        }
                    } finally {
                        lock.unlock();
                    }

                    if(newEvents != null) {
                        //TODO decide on LB with eventsInGroup and newEvents
                        eventsInGroup.addAll(newEvents);
                        boolean isProperlyLoadBalanced = isProperlyLoadBalanced(eventsInGroup.toArray(
                                new MonitoringEvent[eventsInGroup.size()]));
                        if (isProperlyLoadBalanced) {
                            //get avg and create profiled events with machine events
                        } else {
                            //TODO analyze and request. ie: avg(10% to 90%) percentiles of resource utilizations
                            monitoringHandler.addInterestedEvent(groupId, newInterests.toArray(
                                    new InterestedEvent[newInterests.size()]), timeOutForNewEvents);    //TODO work on partitionSize

                            try {
                                Thread.sleep(timeOutForNewEvents);
                            } catch (InterruptedException e) {
                                log.warn("Thread sleep interrupted while waiting for new interested events, part 2");
                            }

                            try {
                                lock.lock();
                                newEvents = eventsToBeProfiledTempMap.get(groupId);
                            } finally {
                                lock.unlock();
                            }

                            //TODO create profiledEvents based on new events (with resource AVGs and machine Events
                        }
                    }
                }

                /* start of dynamic profiling */
                //1) profile events and decide what to request

                //2-a) if properly LB, get avg and create Profiled events & notify listeners
                //2-b) if not properly LB, ask the avg of nodes after removing outliers ( AVG from 10% to 90%)
                /* end of dynamic profiling */

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
        }, 10, windowSize);
    }

    private boolean isProperlyLoadBalanced(MonitoringEvent[] events) {
        boolean isLoadBalanced = false;
        //TODO algo
        return isLoadBalanced;
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
        //////String profiledEventKey = getProfiledEventKey(groupId, monitoringEvent.getClass());

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
