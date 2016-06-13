package se.kth.honeytap.scaling.profile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.Constants;
import se.kth.honeytap.scaling.core.HoneyTapManager;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.scaling.monitoring.MachineMonitoringEvent;
import se.kth.honeytap.scaling.monitoring.MonitoringEvent;
import se.kth.honeytap.scaling.monitoring.MonitoringHandler;
import se.kth.honeytap.scaling.monitoring.ResourceAggregatedEvent;
import se.kth.honeytap.scaling.monitoring.ResourceMonitoringEvent;
import se.kth.honeytap.scaling.monitoring.RuleSupport;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//import HoneyTapManager;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class DynamicEventProfiler {

    Log log = LogFactory.getLog(DynamicEventProfiler.class);

    //<groupId> <MonitoringEvent ArrayList> maps
    private Map<String, ArrayList<MonitoringEvent>> eventsToBeProfiled = new HashMap<String, ArrayList<MonitoringEvent>>();
    private Map<String, ArrayList<MonitoringEvent>> eventsToBeProfiledTempMap = new HashMap<String, ArrayList<MonitoringEvent>>();

    private ArrayList<ProfiledEventListener> profiledEventListeners = new ArrayList<ProfiledEventListener>();
    private HoneyTapManager honeyTapManager;
    private MonitoringHandler monitoringHandler;

    //TODO make this configurable   (Temporary made this static to refer in test)
    private static int windowSize = (10 * 1000)/60; //time in milliseconds
    private int windowFractionSize = windowSize/3;
    private boolean isProcessingInProgress;
    private Lock tempMaplock = new ReentrantLock();

    public DynamicEventProfiler(MonitoringHandler monitoringHandler, HoneyTapManager honeyTapManager) {
        this.monitoringHandler = monitoringHandler;
        this.honeyTapManager = honeyTapManager;
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
                //////isProcessingInProgress = true;

                //TODO process each group in separate thread AND make required methods SYNCHRONIZED... MAY BE ALL :D
                for (String groupId : eventsToBeProfiled.keySet()) {

                    //wait until the events for first fraction of the window received
                    try {
                        Thread.sleep(windowFractionSize);
                    } catch (InterruptedException e) {
                        log.warn("Thread sleep interrupted while waiting for interested events, part 1");
                    }

                    isProcessingInProgress = true;
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
                            honeyTapManager.getNoOfMachinesInGroup(groupId), 0.05f);

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
                            ProfiledEvent profiledEvent = getProfiledEvent(groupId, eventsInGroup);
                            notifyListeners(profiledEvent);
                        }
                    } else {
                        //Step 2-b
                        //not perfectly load balanced, request lowered thresholds
                        try {
                            honeyTapManager.addNewThresholdResourceInterests(groupId, 10f, windowFractionSize); //TODO work on partitionSize
                        } catch (HoneyTapException e) {
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
                        loadBalanced = isProperlyLoadBalanced(eventsInGroup, honeyTapManager.getNoOfMachinesInGroup(groupId), 0.05f);

                        if (loadBalanced) {
                            //2-b-i
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
                                ProfiledEvent profiledEvent = getProfiledEvent(groupId, eventsInGroup);
                                notifyListeners(profiledEvent);
                            }
                        } else {
                            //2-b-ii
                            //TODO - MINOR (coz values won't me much diff if LB. add only the non balanced resources to get AVG(10-90) and change the logic in creating ProfiledEvent too
                            for (RuleSupport.ResourceType resourceType : RuleSupport.ResourceType.values()) {
                                try {
                                    honeyTapManager.addAverageResourceInterests(groupId, resourceType, 10, 90, windowFractionSize); //TODO work on partition
                                } catch (HoneyTapException e) {
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
                                ProfiledEvent profiledEvent = getProfiledEvent(groupId, eventsInGroup);
                                notifyListeners(profiledEvent);
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

    /**
     *
     * @param events
     * @param noOfMachinesInGroup
     * @param ignorableErrorPercentage if it should bear 5% error, should send 0.05as error per
     * @return
     */
    private boolean isProperlyLoadBalanced(ArrayList<MonitoringEvent> events, int noOfMachinesInGroup, float ignorableErrorPercentage) {
        boolean isLoadBalanced = false;

        if (events == null) {
            return isLoadBalanced;
        }

        HashMap<String, ArrayList<ResourceMonitoringEvent>> categorizedResourceEvents = categorizeEventsByTypeNComparator(
                filterNGetResourceEvents(events), Arrays.asList(RuleSupport.ResourceType.values()));

        for (String key : categorizedResourceEvents.keySet()) {
            //check whether each category is load balanced
            ArrayList<String> triggeredMachineIds = new ArrayList<String>();
            for (ResourceMonitoringEvent event : categorizedResourceEvents.get(key)) {
                if (!triggeredMachineIds.contains(event.getMachineId())) {
                    triggeredMachineIds.add(event.getMachineId());
                }
            }

            float ignorableError = ignorableErrorPercentage * noOfMachinesInGroup;
            if ((triggeredMachineIds.size() + ignorableError) < noOfMachinesInGroup) {
                isLoadBalanced = false;
                return isLoadBalanced;
            }
        }

        isLoadBalanced = true;
        return isLoadBalanced;
    }

    private ArrayList<ResourceMonitoringEvent> filterNGetResourceEvents(ArrayList<MonitoringEvent> events) {
        ArrayList<ResourceMonitoringEvent> resourceEvents = new ArrayList<ResourceMonitoringEvent>();
        for (MonitoringEvent event : events) {
            if (event instanceof ResourceMonitoringEvent) {
                resourceEvents.add((ResourceMonitoringEvent)event);
            }
        }
        return resourceEvents;
    }

    private ProfiledEvent getProfiledEvent(String groupId, ArrayList<MonitoringEvent> events) {
        //TODO implement method prioretizing AVG, and adding machine events
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

        ProfiledResourceEvent profiledResourceEvent = null;

        if (resourceAggregatedEvents.size() > 0 || resourceMonitoringEvents.size() > 0) {
            profiledResourceEvent = new ProfiledResourceEvent(groupId);
            List<RuleSupport.ResourceType> requiredResourceTypes = Arrays.asList(RuleSupport.ResourceType.values());
            ArrayList<RuleSupport.ResourceType> profiledResourceTypes = new ArrayList<RuleSupport.ResourceType>();
            String resourceComparatorKey;

            for (ResourceAggregatedEvent aggregatedEvent : resourceAggregatedEvents) {
                resourceComparatorKey = aggregatedEvent.getResourceType().name().concat(Constants.SEPARATOR).concat("AVG");
                profiledResourceEvent.addResourceThresholds(resourceComparatorKey, aggregatedEvent.getCurrentValue());
                profiledResourceTypes.add(aggregatedEvent.getResourceType());
            }

            //fill the resource types of profiled resource with normal resource events
            requiredResourceTypes.removeAll(profiledResourceTypes);
            HashMap<String, ArrayList<ResourceMonitoringEvent>> categorization = categorizeEventsByTypeNComparator(
                    resourceMonitoringEvents, requiredResourceTypes);
            for (String key : categorization.keySet()) {
                float average = getAvgOfCurrentValues(categorization.get(key));
                profiledResourceEvent.addResourceThresholds(key, average);
            }
        }

        if (machineMonitoringEvents.size() > 0) {
            ProfiledEvent profiledMachineEvent = new ProfiledMachineEvent(groupId, machineMonitoringEvents.toArray(
                    new MachineMonitoringEvent[machineMonitoringEvents.size()]), profiledResourceEvent);
            return profiledMachineEvent;
        }
        return profiledResourceEvent;
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

    /**
     * Key of returned hashmap ie: CPU:>=
     * @param resourceEvents
     * @param requestedTypes
     * @return
     */
    private HashMap<String, ArrayList<ResourceMonitoringEvent>> categorizeEventsByTypeNComparator(
            List<ResourceMonitoringEvent> resourceEvents, List<RuleSupport.ResourceType> requestedTypes) {

        HashMap<String, ArrayList<ResourceMonitoringEvent>> categorizedEvents = new HashMap<String, ArrayList<ResourceMonitoringEvent>>();

        for (ResourceMonitoringEvent event : resourceEvents) {
            RuleSupport.ResourceType resourceType = event.getResourceType();

            if (requestedTypes.contains(resourceType)) {
                String key = event.getResourceType().name().concat(Constants.SEPARATOR).concat(
                        RuleSupport.getNormalizedComparatorType(event.getComparator()).name());

                ArrayList<ResourceMonitoringEvent> eventsOfTypeNComparator;
                if(categorizedEvents.containsKey(key)) {
                    eventsOfTypeNComparator = categorizedEvents.get(key);
                } else {
                    eventsOfTypeNComparator = new ArrayList<ResourceMonitoringEvent>();
                }
                eventsOfTypeNComparator.add(event);
                categorizedEvents.put(key, eventsOfTypeNComparator);
            }
        }
        return categorizedEvents;
    }



    private float getAvgOfCurrentValues(ArrayList<ResourceMonitoringEvent> resourceEvents) {
        float sum = 0f;
        for (ResourceMonitoringEvent event : resourceEvents) {
            sum += event.getCurrentValue();
        }
        return sum/resourceEvents.size();
    }

    private void notifyListeners(ProfiledEvent profiledEvent) {
        System.out.println("================ notifying profiled event listeners by Dynamic event profiler =============");

        for (ProfiledEventListener listener : profiledEventListeners) {
            try {
                if (profiledEvent instanceof ProfiledResourceEvent && listener instanceof HoneyTapManager.ProfiledResourceEventListener) {
                    HoneyTapManager.ProfiledResourceEventListener resourceEventListener = (HoneyTapManager.ProfiledResourceEventListener) listener;
                    ProfiledResourceEvent resourceEvent = (ProfiledResourceEvent) profiledEvent;
                    resourceEventListener.handleEvent(resourceEvent);
                } else if (profiledEvent instanceof ProfiledMachineEvent && listener instanceof HoneyTapManager.ProfiledMachineEventListener) {
                    HoneyTapManager.ProfiledMachineEventListener machineEventListener = (HoneyTapManager.ProfiledMachineEventListener) listener;
                    ProfiledMachineEvent machineEvent = (ProfiledMachineEvent) profiledEvent;
                    machineEventListener.handleEvent(machineEvent);
                }

            } catch(HoneyTapException e){
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
