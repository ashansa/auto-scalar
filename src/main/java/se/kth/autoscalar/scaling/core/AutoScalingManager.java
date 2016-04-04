package se.kth.autoscalar.scaling.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.scaling.monitoring.InterestedEvent;
import se.kth.autoscalar.scaling.monitoring.MachineMonitoringEvent;
import se.kth.autoscalar.scaling.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.monitoring.MonitoringListener;
import se.kth.autoscalar.scaling.ScalingSuggestion;
import se.kth.autoscalar.scaling.cost.mgt.KaramelMachineProposer;
import se.kth.autoscalar.scaling.cost.mgt.MachineProposer;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.group.GroupManager;
import se.kth.autoscalar.scaling.group.GroupManagerImpl;
import se.kth.autoscalar.scaling.models.MachineType;
import se.kth.autoscalar.scaling.models.RuntimeGroupInfo;
import se.kth.autoscalar.scaling.profile.*;
import se.kth.autoscalar.scaling.rules.Rule;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class AutoScalingManager {

    Log log = LogFactory.getLog(AutoScalingManager.class);

    GroupManager groupManager;
    EventProfiler eventProfiler;
    MonitoringListener monitoringListener;
    ScaleOutDecisionMaker scaleOutDecisionMaker;
    private boolean optimizedScaleInTmp = true;

    private Map<String, RuntimeGroupInfo> activeGroupsInfo = new HashMap<String, RuntimeGroupInfo>();
    private Map<String, ArrayBlockingQueue<ScalingSuggestion>> suggestionMap = new HashMap<String, ArrayBlockingQueue<ScalingSuggestion>>();
    private ArrayBlockingQueue<String> scaleOutInternalQueue = new ArrayBlockingQueue<String>(1000); //String with the form   <groupId>:<noOfMachines>
    private ArrayBlockingQueue<String> scaleInInternalQueue = new ArrayBlockingQueue<String>(1000); //String with the form   <groupId>:<machineId>

    //private ArrayList<String> activeESGroups = new ArrayList<String>();
    //ArrayBlockingQueue<ScalingSuggestion> suggestionsQueue = new ArrayBlockingQueue<ScalingSuggestion>(50);

    public AutoScalingManager(AutoScalarAPI autoScalarAPI) throws AutoScalarException {
        groupManager = GroupManagerImpl.getInstance();
        eventProfiler = new EventProfiler();
        eventProfiler.addListener(new ProfiledResourceEventListener());
        eventProfiler.addListener(new ProfiledMachineEventListener());
        scaleOutDecisionMaker = new ScaleOutDecisionMaker();
        //scaleInDecisionMaker = new ScaleInDecisionMaker();   //avoid using a different thread until there is a real need
        monitoringListener = new MonitoringListener(autoScalarAPI);

        //starting decision maker threads
        (new Thread(scaleOutDecisionMaker)).start();


    }

    public InterestedEvent[] startAutoScaling(String groupId, int currentNumberOfMachines) throws AutoScalarException {
        addGroupForScaling(groupId, currentNumberOfMachines);
        ArrayList<InterestedEvent> interestedEvents = new ArrayList<InterestedEvent>();
        //TODO add events to list
        return interestedEvents.toArray(new InterestedEvent[interestedEvents.size()]);
    }

    private void addGroupForScaling(String groupId, int currentNumberOfMachines) throws AutoScalarException {
        if (!activeGroupsInfo.containsKey(groupId)) {
            RuntimeGroupInfo runtimeGroupInfo = new RuntimeGroupInfo(groupId, currentNumberOfMachines);
            activeGroupsInfo.put(groupId, runtimeGroupInfo);
        } else {
            throw new AutoScalarException("Group with id " + groupId + " already exists in elastic scalar");
        }
    }

    public void removeGroupFromScaling(String groupId) {
        if (activeGroupsInfo.containsKey(groupId))
            activeGroupsInfo.remove(groupId);
    }

    public ArrayBlockingQueue<ScalingSuggestion> getSuggestions(String groupId) {
        return suggestionMap.get(groupId);
    }

    public MonitoringListener getMonitoringListener() {
        return monitoringListener;
    }

    /**
     * Advice on how many number of machines should be added to the system to cater resource requirements
     */
    public class ProfiledResourceEventListener implements ProfiledEventListener {

        public void handleEvent(ProfiledEvent profiledEvent) throws AutoScalarException {
            if (profiledEvent instanceof ProfiledResourceEvent) {
                ProfiledResourceEvent event = (ProfiledResourceEvent)profiledEvent;
                String groupId = event.getGroupId();
                if(activeGroupsInfo.containsKey(groupId)) {
                    boolean inCoolDownPeriod = isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_OUT);

                    if (!inCoolDownPeriod) {
                        int maxChangeOfMachines = getNumberOfMachineChanges(event);
                        if (maxChangeOfMachines > 0) {
                            RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(groupId);
                            scaleOutInternalQueue.add(groupId.concat(":").concat(String.valueOf(maxChangeOfMachines)));
                            runtimeGroupInfo.setScaleOutInfo(maxChangeOfMachines);
                        } else {
                            if (optimizedScaleInTmp) {
                                //no else part
                                // Scale in will trigger only at the end of a billing period of a machine. This is handled by ProfiledMachineEventListener
                            } else {
                                RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(groupId);
                                ScalingSuggestion suggestion = new ScalingSuggestion(maxChangeOfMachines);

                                ArrayBlockingQueue<ScalingSuggestion> suggestionsQueue;
                                if (suggestionMap.containsKey(groupId)) {
                                    suggestionsQueue = suggestionMap.get(groupId);
                                } else {
                                    suggestionsQueue = new ArrayBlockingQueue<ScalingSuggestion>(50);   //TODO: make 50 configurable
                                }
                                suggestionsQueue.add(suggestion);
                                suggestionMap.put(groupId, suggestionsQueue);

                                runtimeGroupInfo.setScaleInInfo(maxChangeOfMachines);
                            }
                        }
                    }
                } else {
                    throw new AutoScalarException("Resource event cannot be handled. Group is not in active scaling groups." +
                            " Group Id: " + event.getGroupId());
                }
            }
        }
    }

    /**
     *
     * @param event
     * @return number of machines to be added/removed depending on the event and resource rules assigned to the group
     * @throws AutoScalarException
     */
    private int getNumberOfMachineChanges(ProfiledResourceEvent event) throws AutoScalarException {
        Rule[] matchingRules = groupManager.getMatchingRulesForGroup(event.getGroupId(), event.getResourceType(),
                event.getComparator(), event.getValue());
        int maxChangeOfMachines = 0;

        if(RuleSupport.Comparator.GREATER_THAN.equals(event.getComparator()) || RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.equals(event.getComparator())) {
            //will keep the maximum machine additions
            for (Rule rule : matchingRules) {
                if (maxChangeOfMachines < rule.getOperationAction())
                    maxChangeOfMachines = rule.getOperationAction();
            }
        } else if(RuleSupport.Comparator.LESS_THAN.equals(event.getComparator()) || RuleSupport.Comparator.LESS_THAN_OR_EQUAL.equals(event.getComparator())) {
            //will keep the maximum machine removals
            for (Rule rule : matchingRules) {
                if (maxChangeOfMachines > rule.getOperationAction())
                    maxChangeOfMachines = rule.getOperationAction();
            }
        }
        return maxChangeOfMachines;
    }

    public class ProfiledMachineEventListener implements ProfiledEventListener {

        public void handleEvent(ProfiledEvent profiledEvent) throws AutoScalarException {
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

                    //int machineChangesDone = handleWithAssumption2(endOfBillingMachineIds, event);
                    int machineChangesDone = handleWithAssumption1(killedInstances, endOfBillingMachineIds, event);
                    if (machineChangesDone > 0) {
                        runtimeGroupInfo.setScaleOutInfo(machineChangesDone);
                        runtimeGroupInfo.setScaleInInfo(killedInstances);
                    } else if (machineChangesDone < 0){
                        //TODO may need to change this based on assumption 1 or 2 : I think no need since machineChangesDone is returned based on assumption
                        runtimeGroupInfo.setScaleInInfo(machineChangesDone + killedInstances);
                    }
                } else {
                    throw new AutoScalarException("Machine event cannot be handled. Group is not in active scaling groups." +
                            " Group Id: " + event.getGroupId());
                }
            }
        }

        //Assumption1: effect of killed machines are not reflected in resource events
        private int handleWithAssumption1(int killedInstances, ArrayList<String> endOfBillingMachineIds, ProfiledMachineEvent event) throws AutoScalarException {
            ProfiledResourceEvent resourceEventOfGroup = event.getProfiledResourceEvent();
            String groupId = event.getGroupId();
            RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(groupId);
            int machineChanges = 0;

            if (resourceEventOfGroup != null) {
                int maxChangeOfMachines = getNumberOfMachineChanges(event.getProfiledResourceEvent());

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
        private int handleWithAssumption2(ArrayList<String> endOfBillingMachineIds, ProfiledMachineEvent event) throws AutoScalarException {
            ProfiledResourceEvent resourceEventOfGroup = event.getProfiledResourceEvent();
            String groupId = event.getGroupId();
            RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(groupId);
            int machineChanges = 0;

            if (resourceEventOfGroup != null) {
                int maxChangeOfMachines = getNumberOfMachineChanges(event.getProfiledResourceEvent());

                if (maxChangeOfMachines > 0) {
                    //adding only the maxChangeOfMachines to be spawned. because: Assumption2
                    boolean inCoolDownPeriod = isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_OUT);

                    if (!inCoolDownPeriod) {
                        scaleOutInternalQueue.add(groupId.concat(":").concat(String.valueOf(maxChangeOfMachines)));
                        machineChanges = maxChangeOfMachines;
                    }

                } else if (maxChangeOfMachines < 0) {
                    int machinesToBeRemoved = Math.abs(maxChangeOfMachines);
                    if (!isInCoolDownPeriod(groupId, ScalingSuggestion.ScalingDirection.SCALE_IN)) {
                        machineChanges = handleScaleIn(groupId, machinesToBeRemoved, endOfBillingMachineIds);
                    }
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
        }

    }

    private boolean isInCoolDownPeriod(String groupId, ScalingSuggestion.ScalingDirection direction) throws AutoScalarException {
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

        MachineProposer machineProposer = new KaramelMachineProposer();  //TODO: get 'which proposer to use' from a config file

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
                } catch (AutoScalarException e) {
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
        }
    }

    //add a ScaleInDecisionMaker class if required

    public EventProfiler getEventProfiler() {
        return eventProfiler;
    }
}
