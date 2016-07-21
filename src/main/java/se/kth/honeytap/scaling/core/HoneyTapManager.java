package se.kth.honeytap.scaling.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.Constants;
import se.kth.honeytap.scaling.ScalingSuggestion;
import se.kth.honeytap.scaling.cost.mgt.KandyMachineProposer;
import se.kth.honeytap.scaling.cost.mgt.MachineProposer;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.scaling.group.Group;
import se.kth.honeytap.scaling.group.GroupManager;
import se.kth.honeytap.scaling.group.GroupManagerImpl;
import se.kth.honeytap.scaling.models.MachineType;
import se.kth.honeytap.scaling.models.RuntimeGroupInfo;
import se.kth.honeytap.scaling.monitoring.ResourceMonitoringEvent;
import se.kth.honeytap.scaling.rules.Rule;
import se.kth.honeytap.scaling.rules.RuleManager;
import se.kth.honeytap.scaling.rules.RuleManagerImpl;
import se.kth.honeytap.scaling.monitoring.InterestedEvent;
import se.kth.honeytap.scaling.monitoring.MachineMonitoringEvent;
import se.kth.honeytap.scaling.monitoring.MonitoringHandler;
import se.kth.honeytap.scaling.monitoring.MonitoringListener;
import se.kth.honeytap.scaling.monitoring.RuleSupport;
import se.kth.honeytap.scaling.profile.DynamicEventProfiler;
import se.kth.honeytap.scaling.profile.ProfiledEvent;
import se.kth.honeytap.scaling.profile.ProfiledEventListener;
import se.kth.honeytap.scaling.profile.ProfiledMachineEvent;
import se.kth.honeytap.scaling.profile.ProfiledResourceEvent;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class HoneyTapManager {

    Log log = LogFactory.getLog(HoneyTapManager.class);

    private GroupManager groupManager;
    private RuleManager ruleManager;
    //private EventProfiler eventProfiler;
    private DynamicEventProfiler eventProfiler;
    private MonitoringHandler monitoringHandler;
    private ScaleOutDecisionMaker scaleOutDecisionMaker;
    private boolean optimizedScaleInTmp = true;

    private Map<String, RuntimeGroupInfo> activeGroupsInfo = new HashMap<String, RuntimeGroupInfo>();
    private Map<String, ArrayBlockingQueue<ScalingSuggestion>> suggestionMap = new HashMap<String, ArrayBlockingQueue<ScalingSuggestion>>();
    private ArrayBlockingQueue<String> scaleOutInternalQueue = new ArrayBlockingQueue<String>(1000); //String with the form   <groupId>:<noOfMachines>
    private ArrayBlockingQueue<String> scaleInInternalQueue = new ArrayBlockingQueue<String>(1000); //String with the form   <groupId>:<machineId>

    //private ArrayList<String> activeESGroups = new ArrayList<String>();
    //ArrayBlockingQueue<ScalingSuggestion> suggestionsQueue = new ArrayBlockingQueue<ScalingSuggestion>(50);

    public HoneyTapManager(MonitoringHandler monitoringHandler) throws HoneyTapException {
        groupManager = GroupManagerImpl.getInstance();
        ruleManager = RuleManagerImpl.getInstance();
        //eventProfiler = new EventProfiler();
        eventProfiler = new DynamicEventProfiler(monitoringHandler, this);
        eventProfiler.addListener(new ProfiledResourceEventListener());
        eventProfiler.addListener(new ProfiledMachineEventListener());
        scaleOutDecisionMaker = new ScaleOutDecisionMaker();
        //scaleInDecisionMaker = new ScaleInDecisionMaker();   //avoid using a different thread until there is a real need
        this.monitoringHandler = monitoringHandler;

        //starting decision maker threads
        (new Thread(scaleOutDecisionMaker)).start();

        log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> optimizedScaleInTmp >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + optimizedScaleInTmp);
    }

    public MonitoringListener startAutoScaling(final String groupId, int currentNumberOfMachines) throws HoneyTapException {
        ArrayList<InterestedEvent> interestedEvents = new ArrayList<InterestedEvent>();
        String[] ruleNames = groupManager.getRulesForGroup(groupId);
        Rule rule;
        //adding resource utilization interests
        for (String ruleName : ruleNames) {
            try {
                rule = ruleManager.getRule(ruleName);
                interestedEvents.add(new InterestedEvent(rule.getResourceType().name().concat(Constants.SEPARATOR).
                        concat(rule.getComparator().name().concat(Constants.SEPARATOR).concat(String.valueOf(
                                rule.getThreshold()))), rule.getComparator()));
            } catch (HoneyTapException e) {
                log.error("Failed to add rule: " + ruleName + " to interested events.");
            }
        }

        //adding machine status interests
        //////////TODO temp removing since tablespoon does not provide them
        /*interestedEvents.add(new InterestedEvent(MachineMonitoringEvent.Status.AT_END_OF_BILLING_PERIOD.name()));
        interestedEvents.add(new InterestedEvent(MachineMonitoringEvent.Status.KILLED.name()));*/

        addGroupForScaling(groupId, currentNumberOfMachines);
        Group group = groupManager.getGroup(groupId);
        if (currentNumberOfMachines < group.getMinInstances() ) {
            Map<Group.ResourceRequirement, Integer> minResourceReq = group.getMinResourceReq();
            MachineType[] machineProposals = MachineProposer.getKandyProposer().getMachineProposals(groupId, minResourceReq,
                    group.getMinInstances() - currentNumberOfMachines, group.getReliabilityReq());
            scaleOutDecisionMaker.addMachinesToSuggestions(groupId, machineProposals);
            int noOfMachinesProposed = machineProposals.length;
            if (noOfMachinesProposed > 0) {
                activeGroupsInfo.get(groupId).setScaleOutInfo(noOfMachinesProposed);
            }
        }

        final MonitoringListener monitoringListener = monitoringHandler.addGroupForMonitoring(groupId,
                interestedEvents.toArray(new InterestedEvent[interestedEvents.size()]));

        /////TODO temp code for testing
        new Thread(){
            public void run() {
                System.out.println("..................... will add an event after 1 min .............group: " + groupId);
                log.info("..................... will add an event after 1 min .............group: " + groupId);
                try {
                    Thread.sleep(1000 * 60);
                    monitoringListener.onHighRam(groupId, new ResourceMonitoringEvent(
                            groupId, "id1", RuleSupport.ResourceType.CPU, RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, 85));
                } catch (InterruptedException e) {
                    System.out.println("................. INTERRUPTED: will add an event after 1 min.................");
                    log.info("................. INTERRUPTED: will add an event after 1 min.................");
                } catch (HoneyTapException e) {
                    System.out.println("................. HoneyTapException while sending dummy high RAM event .............");
                    log.info("................. HoneyTapException while sending dummy high RAM event .............");
                }
            }
        //};
        }.start();


        return monitoringListener;
    }

    private void addGroupForScaling(String groupId, int currentNumberOfMachines) throws HoneyTapException {
        if (!activeGroupsInfo.containsKey(groupId)) {
            RuntimeGroupInfo runtimeGroupInfo = new RuntimeGroupInfo(groupId, currentNumberOfMachines);
            activeGroupsInfo.put(groupId, runtimeGroupInfo);
        } else {
            throw new HoneyTapException("Group with id " + groupId + " already exists in elastic scalar");
        }
    }

    public void stopAutoScaling(String groupId) {
        if (activeGroupsInfo.containsKey(groupId))
            activeGroupsInfo.remove(groupId);
        monitoringHandler.removeGroupFromMonitoring(groupId);
    }

    //TODO check whether the new suggestions will be updated in the prevously returned queue
    public ArrayBlockingQueue<ScalingSuggestion> getSuggestions(String groupId) {
        return suggestionMap.get(groupId);
    }

   /* public MonitoringListener getMonitoringListener() {
        return monitoringListener;
    }*/

    public int getNoOfMachinesInGroup(String groupId) {
        RuntimeGroupInfo runtimeInfo = activeGroupsInfo.get(groupId);
        if (runtimeInfo == null) {
            log.warn("Given group is not in active Auto scaling groups. GroupId: " + groupId);
        }
        return runtimeInfo.getNumberOfMachinesInGroup();
    }

    /**
     * @param groupId
     * @param thresholdChange   should add -x to lower threshold by x
     * @throws HoneyTapException
     */
    public void addNewThresholdResourceInterests(String groupId, float thresholdChange, int timeDuration) throws HoneyTapException {
        ArrayList<InterestedEvent> interestedEvents = new ArrayList<InterestedEvent>();
        String[] ruleNames = groupManager.getRulesForGroup(groupId);
        Rule rule;
        //adding resource utilization interests
        for (String ruleName : ruleNames) {
            try {
                //TODO if there are more than one CPU > rules, add the lowest which will cover others as well
                //TODO - create new topic to be between 70-80 rather than > 70
                rule = ruleManager.getRule(ruleName);
                RuleSupport.Comparator comparator = rule.getComparator();
                //ie: CPU > 80
                if (RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.equals(RuleSupport.getNormalizedComparatorType(comparator))) {
                    float newThreshold = rule.getThreshold() - thresholdChange;
                    //CPU > 20 ==>   CPU >=20 & CPU <=30//CPU:>=:70:<=:80
                    StringBuilder interest = new StringBuilder();
                    interest.append(rule.getResourceType().name()).append(Constants.SEPARATOR).
                            append(RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name()).append(Constants.SEPARATOR).
                            append(newThreshold).append(Constants.SEPARATOR).
                            append(RuleSupport.Comparator.LESS_THAN_OR_EQUAL.name()).append(Constants.SEPARATOR).
                            append(rule.getThreshold());
                    interestedEvents.add(new InterestedEvent(interest.toString(), RuleSupport.Comparator.GREATER_THAN_OR_EQUAL));
                } else if (RuleSupport.Comparator.LESS_THAN_OR_EQUAL.equals(RuleSupport.getNormalizedComparatorType(comparator))) {
                    //CPU < 20 ==>   CPU >=20 & CPU <=30    //CPU:>=:20:<=:30
                    float newThreshold = rule.getThreshold() + thresholdChange;
                    StringBuilder interest = new StringBuilder();
                    interest.append(rule.getResourceType().name()).append(Constants.SEPARATOR).
                            append(RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name()).append(Constants.SEPARATOR).
                            append(rule.getThreshold()).append(Constants.SEPARATOR).
                            append(RuleSupport.Comparator.LESS_THAN_OR_EQUAL.name()).append(Constants.SEPARATOR).
                            append(newThreshold);
                    interestedEvents.add(new InterestedEvent(interest.toString(), RuleSupport.Comparator.LESS_THAN_OR_EQUAL));
                }
            } catch (HoneyTapException e) {
                log.error("Failed to add rule: " + ruleName + " to interested events.");
            }
        }

        monitoringHandler.addInterestedEvent(groupId, interestedEvents.toArray(new InterestedEvent[interestedEvents.size()]),
                timeDuration);

    }

  /**
   *
   * @param groupId
   * @param resourceType
   * @param lowerPercentile
   * @param upperPercentile
   * @param timeDuration in seconds
   * @throws HoneyTapException
   */
    public void addAverageResourceInterests(String groupId, RuleSupport.ResourceType resourceType, float lowerPercentile,
                                            float upperPercentile, int timeDuration) throws HoneyTapException {

        //ie: CPU:AVG:10-90
        InterestedEvent event = new InterestedEvent(resourceType.name().concat(Constants.SEPARATOR).concat(
                Constants.AVERAGE).concat(Constants.SEPARATOR).concat(String.valueOf(lowerPercentile).concat("-").
                concat(String.valueOf(upperPercentile))));
        monitoringHandler.addInterestedEvent(groupId, new InterestedEvent[]{event}, timeDuration);
    }

    /**
     * Advice on how many number of machines should be added to the system to cater resource requirements
     */
    public class ProfiledResourceEventListener implements ProfiledEventListener {

        public void handleEvent(ProfiledEvent profiledEvent) throws HoneyTapException {
            if (profiledEvent instanceof ProfiledResourceEvent) {
                ProfiledResourceEvent event = (ProfiledResourceEvent)profiledEvent;
                String groupId = event.getGroupId();
                if(activeGroupsInfo.containsKey(groupId)) {
                    //////boolean inCoolDownPeriod = isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_OUT);
                    RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(groupId);
                    Group group = groupManager.getGroup(groupId);

                    ///////if (!inCoolDownPeriod) {
                        int maxChangeOfMachines = getNumberOfMachineChanges(event);
                        if (maxChangeOfMachines > 0 && runtimeGroupInfo.getNumberOfMachinesInGroup() < group.getMaxInstances()
                                && !isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_OUT)) {
                            if (group.getMaxInstances() - runtimeGroupInfo.getNumberOfMachinesInGroup() < maxChangeOfMachines) {
                                maxChangeOfMachines = group.getMaxInstances() - runtimeGroupInfo.getNumberOfMachinesInGroup();
                            }
                            scaleOutInternalQueue.add(groupId.concat(":").concat(String.valueOf(maxChangeOfMachines)));
                            runtimeGroupInfo.setScaleOutInfo(maxChangeOfMachines);
                        } else if (maxChangeOfMachines < 0 && runtimeGroupInfo.getNumberOfMachinesInGroup() > group.getMinInstances()
                                && !isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_IN)) {
                            if (optimizedScaleInTmp) {
                                //no else part
                                // Scale in will trigger only at the end of a billing period of a machine. This is handled by ProfiledMachineEventListener
                            } else {
                                if (runtimeGroupInfo.getNumberOfMachinesInGroup() - group.getMinInstances() < Math.abs(maxChangeOfMachines)) {
                                    maxChangeOfMachines = (-1) * (runtimeGroupInfo.getNumberOfMachinesInGroup() - group.getMinInstances());
                                }
                                ScalingSuggestion suggestion = new ScalingSuggestion(maxChangeOfMachines);

                                ArrayBlockingQueue<ScalingSuggestion> suggestionsQueue;
                                if (suggestionMap.containsKey(groupId)) {
                                    suggestionsQueue = suggestionMap.get(groupId);
                                } else {
                                    suggestionsQueue = new ArrayBlockingQueue<ScalingSuggestion>(50);   //TODO: make 50 configurable
                                }
                                suggestionsQueue.add(suggestion);
                                suggestionMap.put(groupId, suggestionsQueue);
                                log.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ scale-in suggestion added @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + System.currentTimeMillis());

                                runtimeGroupInfo.setScaleInInfo(maxChangeOfMachines);
                            }
                        }
                    /////}
                } else {
                    throw new HoneyTapException("Resource event cannot be handled. Group is not in active scaling groups." +
                            " Group Id: " + event.getGroupId());
                }
            }
        }
    }

    /**
     *
     * @param event
     * @return number of machines to be added/removed depending on the event and resource rules assigned to the group
     * @throws HoneyTapException
     */
    public int getNumberOfMachineChanges(ProfiledResourceEvent event) throws HoneyTapException {
        //TODO should iterate all resource types and comparators and give the machine changes
        ////Set<Rule> allMatchingRules = new HashSet<Rule>();
        HashMap<String, Float> resourceThresholds = event.getResourceThresholds();   //ie key: CPU:>=
        ArrayList<Integer> noOfMachineChanges = new ArrayList<Integer>();
        int maxChangeOfMachines = 0;

        for (String resourceComparatorKey : resourceThresholds.keySet()) {
            RuleSupport.ResourceType resourceType = RuleSupport.ResourceType.valueOf(resourceComparatorKey.split(Constants.SEPARATOR)[0]);
            RuleSupport.Comparator comparator = RuleSupport.Comparator.valueOf(resourceComparatorKey.split(Constants.SEPARATOR)[1]);

            Rule[] matchingRules = groupManager.getMatchingRulesForGroup(event.getGroupId(), resourceType, comparator,
                    resourceThresholds.get(resourceComparatorKey));

            if(RuleSupport.Comparator.GREATER_THAN.equals(comparator) || RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.equals(comparator)) {
                //will keep the maximum machine additions
                for (Rule rule : matchingRules) {
                    if (maxChangeOfMachines < rule.getOperationAction())
                        maxChangeOfMachines = rule.getOperationAction();
                }
            } else if(RuleSupport.Comparator.LESS_THAN.equals(comparator) || RuleSupport.Comparator.LESS_THAN_OR_EQUAL.equals(comparator)) {
                //will keep the maximum machine removals
                for (Rule rule : matchingRules) {
                    if (maxChangeOfMachines > rule.getOperationAction())
                        maxChangeOfMachines = rule.getOperationAction();
                }
            }
            //TODO test this method, maxChangeOfMachines was 0
            noOfMachineChanges.add(maxChangeOfMachines);
        }
        return Collections.max(noOfMachineChanges);
    }

    public class ProfiledMachineEventListener implements ProfiledEventListener {

        public void handleEvent(ProfiledEvent profiledEvent) throws HoneyTapException {
            if (profiledEvent instanceof ProfiledMachineEvent) {
                ProfiledMachineEvent event = (ProfiledMachineEvent)profiledEvent;
                if (activeGroupsInfo.containsKey(event.getGroupId())) {
                    int killedInstances = 0;
                    ArrayList<String> endOfBillingMachineIds = new ArrayList<String>();
                    for (MachineMonitoringEvent machineEvent : event.getMachineMonitoringEvents()) {
                        switch (machineEvent.getStatus()) {
                            case KILLED:
                                killedInstances++;
                                break;
                            case AT_END_OF_BILLING_PERIOD:
                                endOfBillingMachineIds.add(machineEvent.getMachineId());
                                break;
                        }
                    }

                    RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(event.getGroupId());

                    int machineChangesDone = makeScalinDecisionWithAssump2(endOfBillingMachineIds, event);
                    //int machineChangesDone = handleWithAssumption1(killedInstances, endOfBillingMachineIds, event);
                    if (machineChangesDone > 0) {
                        runtimeGroupInfo.setScaleOutInfo(machineChangesDone);
                        if (killedInstances > 0) {
                            runtimeGroupInfo.setScaleInInfo(killedInstances);
                        }
                    } else if (machineChangesDone < 0){
                        //TODO may need to change this based on assumption 1 or 2 : I think no need since machineChangesDone is returned based on assumption
                        runtimeGroupInfo.setScaleInInfo(Math.abs(machineChangesDone) + killedInstances);
                    }
                } else {
                    throw new HoneyTapException("Machine event cannot be handled. Group is not in active scaling groups." +
                            " Group Id: " + event.getGroupId());
                }
            }
        }

        //Assumption1: effect of killed machines are not reflected in resource events
        private int handleWithAssumption1(int killedInstances, ArrayList<String> endOfBillingMachineIds, ProfiledMachineEvent event) throws HoneyTapException {
            ProfiledResourceEvent profiledResourceEventOfGroup = event.getProfiledResourceEvent();
            String groupId = event.getGroupId();
            RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(groupId);
            int machineChanges = 0;

            if (profiledResourceEventOfGroup != null) {
                int maxChangeOfMachines = getNumberOfMachineChanges(profiledResourceEventOfGroup);

                if (maxChangeOfMachines > 0) {
                    //Adding the machinesKilled + maxChangeOfMachines to be spawned: because Assumption1
                    int machinesToBeAdded = maxChangeOfMachines + killedInstances;
                    boolean inCoolDownPeriod = isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_OUT);

                    if (!inCoolDownPeriod) {
                        scaleOutInternalQueue.add(groupId.concat(":").concat(String.valueOf(machinesToBeAdded)));
                        machineChanges = machinesToBeAdded;
                    }
                } else if (maxChangeOfMachines < 0) {
                    int machinesToBeRemoved = Math.abs(maxChangeOfMachines);
                    if (machinesToBeRemoved > killedInstances) {
                        if (!isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_IN)) {
                            machinesToBeRemoved = machinesToBeRemoved - killedInstances;
                            machineChanges = handleScaleIn(groupId, machinesToBeRemoved, endOfBillingMachineIds);
                        } //will not scale in since in cooldown period
                    } else {
                        int machinesToBeAdded = killedInstances - machinesToBeRemoved;
                        boolean inCoolDownPeriod = isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_OUT);
                        if (!inCoolDownPeriod) {
                            scaleOutInternalQueue.add(groupId.concat(":").concat(String.valueOf(machinesToBeAdded)));
                            machineChanges = machinesToBeAdded;
                        }
                    }
                }
            } else {
                //Since no low resource consumption events are in the window (no resource events), adding machines to replace killed machines
                boolean inCoolDownPeriod = isInCoolDownPeriod(event.getGroupId(), ScalingSuggestion.ScalingDirection.SCALE_OUT);
                if (!inCoolDownPeriod) {
                    scaleOutInternalQueue.add(event.getGroupId().concat(":").concat(String.valueOf(killedInstances)));
                    machineChanges = killedInstances;
                }
            }
            return machineChanges;
        }

        //Assumption2: effect of killed machines are reflected in resource events. Hence can act on what rules propose
        private int makeScalinDecisionWithAssump2(ArrayList<String> endOfBillingMachineIds, ProfiledMachineEvent event) throws HoneyTapException {
            ProfiledResourceEvent resourceEventOfGroup = event.getProfiledResourceEvent();
            String groupId = event.getGroupId();
            RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(groupId);
            Group group = groupManager.getGroup(event.getGroupId());
            int machineChanges = 0;

            if (resourceEventOfGroup != null) {
                int requiredMachineChanges = getNumberOfMachineChanges(event.getProfiledResourceEvent());

                if (requiredMachineChanges > 0 && runtimeGroupInfo.getNumberOfMachinesInGroup() < group.getMaxInstances()
                        && !isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_OUT)) {
                    //adding only the maxChangeOfMachines to be spawned. because: Assumption2
                        scaleOutInternalQueue.add(groupId.concat(":").concat(String.valueOf(requiredMachineChanges)));
                        machineChanges = requiredMachineChanges;

                } else if (requiredMachineChanges < 0 && runtimeGroupInfo.getNumberOfMachinesInGroup() > group.getMinInstances()
                        && !isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_IN)) {
                    int machinesToBeRemoved = Math.abs(requiredMachineChanges);
                    machineChanges = handleScaleIn(groupId, machinesToBeRemoved, endOfBillingMachineIds);

                }
            }//else do nothing. Since no resource events: load not high or low
            return machineChanges;
        }

        private int handleScaleIn(String groupId, int machinesToBeRemoved, ArrayList<String> endOfBillingMachineIds) {
            int machineChanges = 0;
            if (optimizedScaleInTmp) {
                if (machinesToBeRemoved >= endOfBillingMachineIds.size()) {
                    //kill all machines at end of billing period. (we won't kill machines which are not at the
                    //end of billing period to utilize the already payed machines
                    addScaleInSuggestions(groupId, endOfBillingMachineIds);
                    machineChanges = (-1) * endOfBillingMachineIds.size();
                        /* won't add to internal queue and wait for queue in while loop until there is a real need
                            scaleInInternalQueue.addAll(endOfBillingMachineIds); */
                } else {
                    //kill number of machinesToBeRemoved selected from endOfBilling set
                    ArrayList<String> toRemoveList = new ArrayList<String>(endOfBillingMachineIds.subList(0, machinesToBeRemoved));
                    addScaleInSuggestions(groupId, toRemoveList);
                    machineChanges = (-1) * toRemoveList.size();
                        /* won't add to internal queue and wait for queue in while loop until there is a real need
                            scaleInInternalQueue.addAll(endOfBillingMachineIds);
                            scaleInInternalQueue.add(new ArrayList<String>(endOfBillingMachineIds.subList(0, machinesToBeRemoved)));*/
                }
            } else {
                ScalingSuggestion suggestion = new ScalingSuggestion((-1) * machinesToBeRemoved);

                ArrayBlockingQueue<ScalingSuggestion> suggestionsQueue;
                if (suggestionMap.containsKey(groupId)) {
                    suggestionsQueue = suggestionMap.get(groupId);
                } else {
                    suggestionsQueue = new ArrayBlockingQueue<ScalingSuggestion>(50);   //TODO: make 50 configurable
                }
                suggestionsQueue.add(suggestion);
                suggestionMap.put(groupId, suggestionsQueue);
                log.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ scalein-tmp suggestion added @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + System.currentTimeMillis());

                machineChanges = (-1) * machinesToBeRemoved;
            }

            return machineChanges;
        }

        /**
         * Until there is a real need for ScaleInDecisionMaker, we'll add suggestions from here than waiting for inernal
         * queue in a while(true) loop
         * @param groupId
         * @param machinesToBeRemoved
         */
        private void addScaleInSuggestions(String groupId, ArrayList<String> machinesToBeRemoved) {
            ScalingSuggestion suggestion = new ScalingSuggestion(machinesToBeRemoved.toArray(new String[machinesToBeRemoved.size()]));

            ArrayBlockingQueue<ScalingSuggestion> suggestionsQueue;
            if (suggestionMap.containsKey(groupId)) {
                suggestionsQueue = suggestionMap.get(groupId);
            } else {
                suggestionsQueue = new ArrayBlockingQueue<ScalingSuggestion>(50);   //TODO: make 50 configurable
            }
            suggestionsQueue.add(suggestion);
            suggestionMap.put(groupId, suggestionsQueue);
            log.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ scale-in suggestion added @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + System.currentTimeMillis());

        }

    }

    private boolean isInCoolDownPeriod(String groupId, ScalingSuggestion.ScalingDirection direction) throws HoneyTapException {
        RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(groupId);

        //Evaluate the possibility of scaling with coolDown period
        boolean isInCoolDownPeriod = true;
        Date timeOfLastDecision;
        long timeDiffSeconds;
        if(ScalingSuggestion.ScalingDirection.SCALE_IN.equals(direction)) {
            timeOfLastDecision = runtimeGroupInfo.getLastScaleInTime();
            if (timeOfLastDecision == null) {
                isInCoolDownPeriod = false;
            } else {
                timeDiffSeconds = (Calendar.getInstance().getTime().getTime() - timeOfLastDecision.getTime())/1000;
                if (timeDiffSeconds >= groupManager.getGroup(groupId).getCoolingTimeIn()) {
                    isInCoolDownPeriod = false;
                }
            }
        } else {
            timeOfLastDecision = runtimeGroupInfo.getLastScaleOutTime();
            if (timeOfLastDecision == null) {
                isInCoolDownPeriod = false;
            } else {
                timeDiffSeconds = (Calendar.getInstance().getTime().getTime() - timeOfLastDecision.getTime())/1000;
                if (timeDiffSeconds >= groupManager.getGroup(groupId).getCoolingTimeOut()) {
                    isInCoolDownPeriod = false;
                }
            }
        }
        return isInCoolDownPeriod;
    }

    /**
     * Get the scaleOut suggestions(#of machines to be spawned) and propose the best suitable machine configs
     */
    private class ScaleOutDecisionMaker implements Runnable {

        MachineProposer machineProposer = MachineProposer.getKandyProposer();  //TODO: get 'which proposer to use' from a config file

        public void run() {
            String groupId = "";
            int noOfMachines;
            while (true) {
                try {
                    String suggestion = scaleOutInternalQueue.take(); //suggestion is in the form <groupId>:<noOfMachines>
                    groupId = suggestion.substring(0,suggestion.lastIndexOf(":"));
                    noOfMachines = Integer.parseInt(suggestion.substring(suggestion.lastIndexOf(":") + 1, suggestion.length()));

                    Group group = groupManager.getGroup(groupId);
                    Map<Group.ResourceRequirement, Integer> minResourceReq = group.getMinResourceReq();
                    MachineType[] machineProposals = machineProposer.getMachineProposals(groupId, minResourceReq,
                            noOfMachines, group.getReliabilityReq());
                    addMachinesToSuggestions(groupId, machineProposals);

                } catch (InterruptedException e) {
                    log.error("Error while retrieving item from scaleOutInternalQueue. " + e.getMessage());
                } catch (HoneyTapException e) {
                    log.error("Error while retrieving min resource req of group " + groupId + " ." + e.getMessage());
                }
            }
        }

        private void addMachinesToSuggestions(String groupId, MachineType[] machines) {
            ScalingSuggestion suggestion = new ScalingSuggestion(machines);

            ArrayBlockingQueue<ScalingSuggestion> suggestionsQueue;
            if (suggestionMap.containsKey(groupId)) {
                suggestionsQueue = suggestionMap.get(groupId);
            } else {
                suggestionsQueue = new ArrayBlockingQueue<ScalingSuggestion>(50);   //TODO: make 50 configurable
            }
            suggestionsQueue.add(suggestion);
            suggestionMap.put(groupId, suggestionsQueue);
            log.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ scale-out suggestion added @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " + System.currentTimeMillis());

        }
    }

    //add a ScaleInDecisionMaker class if required

    //public EventProfiler getEventProfiler() {
    public DynamicEventProfiler getEventProfiler() {
        return eventProfiler;
    }
}
