package se.kth.autoscalar.scaling.profile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.scaling.core.AutoScalingManager;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;
import se.kth.autoscalar.scaling.monitoring.*;

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
    private AutoScalingManager autoScalingManager;

    //TODO make this configurable   (Temporary made this static to refer in test)
    private static int windowSize = 30; //time in ms
    private int windowFractionSize = windowSize/3;
    private boolean isProcessingInProgress;
    private Lock tempMaplock = new ReentrantLock();

    public DynamicEventProfiler(MonitoringHandler monitoringHandler, AutoScalingManager autoScalingManager) {
        this.monitoringHandler = monitoringHandler;
        this.autoScalingManager = autoScalingManager;
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

                //TODO process each group in separate thread AND make required methods SYNCHRONIZED... MAY BE ALL :D
                for (String groupId : eventsToBeProfiled.keySet()) {

                    //wait until the events for first fraction of the window received
                    try {
                        Thread.sleep(windowFractionSize);
                    } catch (InterruptedException e) {
                        log.warn("Thread sleep interrupted while waiting for interested events, part 1");
                    }

                    ArrayList<MonitoringEvent> eventsInGroup = eventsToBeProfiled.get(groupId);;

                    /* start of dynamic profiling */
                    //Step 1) profile events
                    //Step 2-a) if properly LB, get avg and create Profiled events & notify listeners
                    //Step 2-b) if not properly LB, lower threshold and request events
                    //Step 2-b-i) if LB, decide on intensity and notify the profiled event with avg + intensity
                    //Step 2-b-ii) if not LB,  ask the avg of nodes after removing outliers ( AVG from 10% to 90%)
                    //decide on intensity and notify the profiled event with ( AVG from 10% to 90%) + intensity

                    /* end of dynamic profiling */

                    //Step 1
                    boolean loadBalanced = isProperlyLoadBalanced(eventsInGroup,
                            autoScalingManager.getNoOfMachinesInGroup(groupId), 5);

                    if (loadBalanced) {
                        //Step 2-a
                        //TODO analyze these results too
                        try {
                            Thread.sleep(windowFractionSize * 2);
                        } catch (InterruptedException e) {
                            log.warn("Thread sleep interrupted while waiting for new interested events, part 1");
                        }

                        ArrayList<MonitoringEvent> newEvents;
                        try {
                            tempMaplock.lock();
                            newEvents = eventsToBeProfiledTempMap.get(groupId);
                        } finally {
                            tempMaplock.unlock();
                        }

                        eventsInGroup = combineEvents(eventsInGroup, newEvents);
                        if (eventsInGroup != null) {
                            ProfiledEvent profiledEvent = getProfiledEvent(eventsInGroup);
                            notifyListeners(profiledEvent);
                        }
                    } else {
                        //Step 2-b
                        //not perfectly load balanced, request lowered thresholds
                        try {
                            autoScalingManager.addNewThresholdResourceInterests(groupId, -10f, windowFractionSize); //TODO work on partitionSize
                        } catch (AutoScalarException e) {
                            log.warn("Error while adding new threshold interests for group: " + groupId);
                        }

                        try {
                            Thread.sleep(windowFractionSize);
                        } catch (InterruptedException e) {
                            log.warn("Thread sleep interrupted while waiting for new interested events, part 1");
                        }

                        ArrayList<MonitoringEvent> newEvents;
                        try {
                            tempMaplock.lock();
                            newEvents = eventsToBeProfiledTempMap.get(groupId);
                            if(newEvents != null) {
                                //add empty array to keep new events
                                eventsToBeProfiledTempMap.put(groupId, new ArrayList<MonitoringEvent>());
                            }
                        } finally {
                            tempMaplock.unlock();
                        }

                        eventsInGroup = combineEvents(eventsInGroup, newEvents);
                        loadBalanced = isProperlyLoadBalanced(eventsInGroup, autoScalingManager.getNoOfMachinesInGroup(groupId), 5);

                        if (loadBalanced) {
                            //2-a-i
                            //get avg and create profiled events with machine events
                            try {
                                Thread.sleep(windowFractionSize);
                            } catch (InterruptedException e) {
                                log.warn("Thread sleep interrupted while waiting for new interested events, part 1");
                            }

                            try {
                                tempMaplock.lock();
                                newEvents = eventsToBeProfiledTempMap.get(groupId);
                            } finally {
                                tempMaplock.unlock();
                            }

                            eventsInGroup = combineEvents(eventsInGroup, newEvents);
                            if (eventsInGroup != null) {
                                ProfiledEvent profiledEvent = getProfiledEvent(eventsInGroup);
                                notifyListeners(profiledEvent);
                            }
                        } else {
                            //2-a-ii
                            //TODO - MINOR (coz values won't me much diff if LB. add only the non balanced resources to get AVG(10-90) and change the logic in creating ProfiledEvent too
                            for (RuleSupport.ResourceType resourceType : RuleSupport.ResourceType.values()) {
                                try {
                                    autoScalingManager.addAverageResourceInterests(groupId, resourceType, 10, 90, windowFractionSize); //TODO work on partition
                                } catch (AutoScalarException e) {
                                    log.warn("Error while adding new AVG(10-90) interest for group: " + groupId +
                                            " for resource type: " + resourceType.name());
                                }
                            }

                            try {
                                Thread.sleep(windowFractionSize);
                            } catch (InterruptedException e) {
                                log.warn("Thread sleep interrupted while waiting for new interested events, part 2");
                            }

                            try {
                                tempMaplock.lock();
                                newEvents = eventsToBeProfiledTempMap.get(groupId);
                            } finally {
                                tempMaplock.unlock();
                            }
                            //create profiledEvents based on new events (with resource AVGs and machine Events
                            eventsInGroup = combineEvents(eventsInGroup, newEvents);
                            if (eventsInGroup != null) {
                                ProfiledEvent profiledEvent = getProfiledEvent(eventsInGroup);
                                notifyListeners(profiledEvent);
                            }
                        }
                    }
                }

                try {
                    tempMaplock.lock();
                    //after processing is done, add events added to temp map to eventsToBeProcessed so that they will
                    // be processed in the next scheduling round
                    eventsToBeProfiled = eventsToBeProfiledTempMap;
                    eventsToBeProfiledTempMap = new HashMap<String, ArrayList<MonitoringEvent>>();
                } finally {
                    tempMaplock.unlock();
                    isProcessingInProgress = false;
                }
            }
        }, 0, windowSize);
    }

    private synchronized ArrayList<MonitoringEvent> combineEvents(ArrayList<MonitoringEvent> existingEvents, ArrayList<MonitoringEvent> newEvents) {
        if (existingEvents != null) {
            if (newEvents != null) {
                existingEvents.addAll(newEvents);
                return existingEvents;
            } else {
                return existingEvents;
            }
        } else {
            return newEvents;
        }
    }

    private boolean isProperlyLoadBalanced(ArrayList<MonitoringEvent> events, int noOfMachinesInGroup, int errorPercentage) {
        boolean isLoadBalanced = false;

        if (events == null) {
            return isLoadBalanced;
        }

        //TODO improve algo
        //MachineID, MonitoringEvents map
        Map<String, ArrayList<MonitoringEvent>> receivedEventsByMachine = new HashMap<String, ArrayList<MonitoringEvent>>();
        for (MonitoringEvent monitoringEvent : events) {
            ArrayList<MonitoringEvent> eventsOfMachine;
            if (receivedEventsByMachine.containsKey(monitoringEvent.getMachineId())) {
                eventsOfMachine = receivedEventsByMachine.get(monitoringEvent.getMachineId());
            } else {
                eventsOfMachine = new ArrayList<MonitoringEvent>();
            }
            eventsOfMachine.add(monitoringEvent);
            receivedEventsByMachine.put(monitoringEvent.getMachineId(), eventsOfMachine);
        }
        if (receivedEventsByMachine.size() >= noOfMachinesInGroup) {
            isLoadBalanced = true;
        }
        return isLoadBalanced;
    }

    private ProfiledEvent getProfiledEvent(ArrayList<MonitoringEvent> events) {
        //TODO implement method priorotizing AVG, and adding machine events
        ArrayList<ResourceMonitoringEvent> resourceMonitoringEvents = new ArrayList<ResourceMonitoringEvent>();
        ArrayList<MachineMonitoringEvent> machineMonitoringEvents = new ArrayList<MachineMonitoringEvent>();
        ArrayList<ResourceAggregatedEvent> resourceAggregatedEvents = new ArrayList<ResourceAggregatedEvent>();

        for (MonitoringEvent event : events) {
            if (event instanceof ResourceMonitoringEvent)
                resourceMonitoringEvents.add((ResourceMonitoringEvent)event);
            else if (event instanceof MachineMonitoringEvent)
                machineMonitoringEvents.add((MachineMonitoringEvent) event);
            else if (event instanceof ResourceAggregatedEvent)
               resourceAggregatedEvents.add((ResourceAggregatedEvent)event);
        }

        //create profiled resource events for resource types supported
        ArrayList<ProfiledResourceEvent> profiledResourceEvents = new ArrayList<ProfiledResourceEvent>();
        ArrayList<RuleSupport.ResourceType> profiledResourceTypes = new ArrayList<RuleSupport.ResourceType>();

        //create profiled resource events with aggregated events
        for (ResourceAggregatedEvent aggregatedEvent : resourceAggregatedEvents) {
            ProfiledResourceEvent profiledResourceEvent = new ProfiledResourceEvent(aggregatedEvent.getGroupId(),
                    aggregatedEvent.getResourceType(), aggregatedEvent.getComparator(), aggregatedEvent.getCurrentValue());
            profiledResourceEvents.add(profiledResourceEvent);
            profiledResourceTypes.add(profiledResourceEvent.getResourceType());
        }
        //fill the profiled resource events with normal resource events
        for (RuleSupport.ResourceType resourceType : RuleSupport.ResourceType.values()) {
            if (!profiledResourceTypes.contains(resourceType)) {

                //TODO ****** should consider the comparator value as well :(
                ResourceMonitoringEvent[] resourceEventsOfType = getResourceEventsOfType(resourceMonitoringEvents.
                        toArray(new ResourceMonitoringEvent[resourceMonitoringEvents.size()]), resourceType);
                if (resourceEventsOfType.length > 0) {
                    float average = getAvgOfCurrentValues(resourceEventsOfType);
                    ResourceMonitoringEvent sampleEvent = resourceEventsOfType[0];  //to retrieve info for Profiled event
                    ProfiledResourceEvent profiledResourceEvent = new ProfiledResourceEvent(sampleEvent.getGroupId(),
                            sampleEvent.getResourceType(), sampleEvent.getComparator(), average);
                    profiledResourceEvents.add(profiledResourceEvent);
                }
            }
        }



        if (machineMonitoringEvents.size() > 0) {
            MachineMonitoringEvent sampleEvent = machineMonitoringEvents.get(0); //to retrieve info for Profiled event
            //ProfiledEvent profiledEvent = new ProfiledMachineEvent(sampleEvent.getGroupId())
        }
        return null;
    }

    private ResourceMonitoringEvent[] getResourceEventsOfType(ResourceMonitoringEvent[] resourceEvents,
                                                              RuleSupport.ResourceType resourceType) {
        ArrayList<ResourceMonitoringEvent> filteredEvents = new ArrayList<ResourceMonitoringEvent>();
        for (ResourceMonitoringEvent resourceEvent : resourceEvents) {
            if (resourceEvent.getResourceType().equals(resourceType)) {
                filteredEvents.add(resourceEvent);
            }
        }
        return filteredEvents.toArray(new ResourceMonitoringEvent[filteredEvents.size()]);
    }

    private float getAvgOfCurrentValues(ResourceMonitoringEvent[] resourceEvents) {
        float sum = 0f;
        for (ResourceMonitoringEvent event : resourceEvents) {
            sum += event.getCurrentValue();
        }
        return sum/resourceEvents.length;
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
            tempMaplock.lock();
            //adding events to temp map since the event map is processing (eventsToBeProfiledTempMap)

            ArrayList<MonitoringEvent> events;
            if (eventsToBeProfiledTempMap.containsKey(groupId)) {
                events = eventsToBeProfiledTempMap.get(groupId);
            } else {
                events = new ArrayList<MonitoringEvent>();
            }

            events.add(monitoringEvent);
            eventsToBeProfiledTempMap.put(groupId, events);

            tempMaplock.unlock();
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

    public static int getWindowSize() {
        return windowSize;
    }
}
