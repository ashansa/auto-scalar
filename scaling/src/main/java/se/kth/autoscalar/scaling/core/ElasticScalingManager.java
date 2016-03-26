package se.kth.autoscalar.scaling.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.common.monitoring.MachineMonitoringEvent;
import se.kth.autoscalar.common.monitoring.MonitoringEvent;
import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.MonitoringListener;
import se.kth.autoscalar.scaling.ScalingSuggestion;
import se.kth.autoscalar.scaling.cost.mgt.KaramelMachineProposer;
import se.kth.autoscalar.scaling.cost.mgt.MachineProposer;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
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
public class ElasticScalingManager {

    Log log = LogFactory.getLog(ElasticScalingManager.class);

    GroupManager groupManager;
    //RuleManager ruleManager;
    EventProfiler eventProfiler;
    //ArrayList<MonitoringListener> listenerArray = new ArrayList<MonitoringListener>();
    MonitoringListener monitoringListener;
    ScaleOutDecisionMaker scaleOutDecisionMaker;
    //ScaleInDecisionMaker scaleInDecisionMaker;

    private Map<String, RuntimeGroupInfo> activeGroupsInfo = new HashMap<String, RuntimeGroupInfo>();
    private Map<String, ArrayBlockingQueue<ScalingSuggestion>> suggestionMap = new HashMap<String, ArrayBlockingQueue<ScalingSuggestion>>();
    private ArrayBlockingQueue<String> scaleOutInternalQueue = new ArrayBlockingQueue<String>(1000); //String with the form   <groupId>:<noOfMachines>
    private ArrayBlockingQueue<String> scaleInInternalQueue = new ArrayBlockingQueue<String>(1000); //String with the form   <groupId>:<machineId>

    //private ArrayList<String> activeESGroups = new ArrayList<String>();
    //ArrayBlockingQueue<ScalingSuggestion> suggestionsQueue = new ArrayBlockingQueue<ScalingSuggestion>(50);

    public ElasticScalingManager(ElasticScalarAPI elasticScalarAPI) throws ElasticScalarException {
        //ruleManager = RuleManagerImpl.getInstance();
        groupManager = GroupManagerImpl.getInstance();
        eventProfiler = new EventProfiler();
        eventProfiler.addListener(new ProfiledResourceEventListener());
        eventProfiler.addListener(new ProfiledMachineEventListener());
        scaleOutDecisionMaker = new ScaleOutDecisionMaker();
        //scaleInDecisionMaker = new ScaleInDecisionMaker();   //avoid using a different thread until there is a real need
        monitoringListener = new MonitoringListener(elasticScalarAPI);

        //starting decision maker threads
        (new Thread(scaleOutDecisionMaker)).start();


    }

    public void addGroupForScaling(String groupId, int currentNumberOfMachines) throws ElasticScalarException {
        if (!activeGroupsInfo.containsKey(groupId)) {
            RuntimeGroupInfo runtimeGroupInfo = new RuntimeGroupInfo(groupId, currentNumberOfMachines);
            activeGroupsInfo.put(groupId, runtimeGroupInfo);
        } else {
            throw new ElasticScalarException("Group with id " + groupId + " already exists in elastic scalar");
        }
    }

    public void removeGroupFromScaling(String groupId) {
        if (activeGroupsInfo.containsKey(groupId))
            activeGroupsInfo.remove(groupId);
    }

    public ArrayBlockingQueue<ScalingSuggestion> getSuggestions(String groupId) {
        return suggestionMap.get(groupId);
    }

    public void handleEvent(String groupId, MonitoringEvent monitoringEvent) throws ElasticScalarException {
        eventProfiler.profileEvent(groupId, monitoringEvent);
    }

    public MonitoringListener getMonitoringListener() {
        return monitoringListener;
    }

    /**
     * Advice on how many number of machines should be added to the system to cater resource requirements
     */
    public class ProfiledResourceEventListener implements ProfiledEventListener {

        public void handleEvent(ProfiledEvent profiledEvent) throws ElasticScalarException {
            if (profiledEvent instanceof ProfiledResourceEvent) {
                ProfiledResourceEvent event = (ProfiledResourceEvent)profiledEvent;
                if(activeGroupsInfo.containsKey(event.getGroupId())) {
                    boolean inCoolDownPeriod = isInCoolDownPeriod(event.getGroupId(), ScalingSuggestion.ScalingDirection.SCALE_OUT);

                    if (!inCoolDownPeriod) {
                        int maxChangeOfMachines = getNumberOfMachineChanges(event);
                        if (maxChangeOfMachines > 0) {
                            //Evaluate the possibility of scaling out with cooldown period
                            RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(event.getGroupId());
                            scaleOutInternalQueue.add(event.getGroupId().concat(":").concat(String.valueOf(maxChangeOfMachines)));
                            runtimeGroupInfo.setScaleOutInfo(maxChangeOfMachines);
                        }
                    }   //no else part
                        // Scale in will trigger only at the end of a billing period of a machine. This is handled by ProfiledMachineEventListener
                } else {
                    throw new ElasticScalarException("Resource event cannot be handled. Group is not in active scaling groups." +
                            " Group Id: " + event.getGroupId());
                }
            }
        }
    }

    /**
     *
     * @param event
     * @return number of machines to be added/removed depending on the event and resource rules assigned to the group
     * @throws ElasticScalarException
     */
    private int getNumberOfMachineChanges(ProfiledResourceEvent event) throws ElasticScalarException {
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

        public void handleEvent(ProfiledEvent profiledEvent) throws ElasticScalarException {
            if (profiledEvent instanceof ProfiledMachineEvent) {
                ProfiledMachineEvent event = (ProfiledMachineEvent)profiledEvent;
                if (activeGroupsInfo.containsKey(event.getGroupId())) {
                    //ProfiledResourceEvent resourceEventOfGroup = event.getProfiledResourceEvent();

                    int killedInstances = 0;
                    ArrayList<String> endOfBillingMachineIds = new ArrayList<String>();
                    for (MachineMonitoringEvent machineEvent : event.getMachineMonitoringEvents()) {
                        switch (machineEvent.getStatus()) {
                            case KILLED:
                                killedInstances++;
                                break;
                            case AT_END_OF_BILLING_PERIOD:
                                //endOfBilling++;
                                endOfBillingMachineIds.add(machineEvent.getMachineId());
                                break;
                        }
                    }

                    //update the killed machines in runtime group info
                    RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(event.getGroupId());

                    //int machineChanges = handleWithAssumption2(endOfBillingMachineIds, event);
                    int machineChanges = handleWithAssumption1(killedInstances, endOfBillingMachineIds, event);
                    if (machineChanges > 0) {
                        runtimeGroupInfo.setScaleOutInfo(machineChanges);
                        runtimeGroupInfo.setScaleInInfo(killedInstances);
                    } else if (machineChanges < 0){
                        //TODO may need to change this based on assumption 1 or 2
                        runtimeGroupInfo.setScaleInInfo(machineChanges + killedInstances);
                    }
                } else {
                    throw new ElasticScalarException("Machine event cannot be handled. Group is not in active scaling groups." +
                            " Group Id: " + event.getGroupId());
                }
            }
        }

        //Assumption1: effect of killed machines are not reflected in resource events
        private int handleWithAssumption1(int killedInstances, ArrayList<String> endOfBillingMachineIds, ProfiledMachineEvent event) throws ElasticScalarException {
            ProfiledResourceEvent resourceEventOfGroup = event.getProfiledResourceEvent();
            RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(event.getGroupId());
            int machineChanges = 0;

            if (resourceEventOfGroup != null) {
                int maxChangeOfMachines = getNumberOfMachineChanges(event.getProfiledResourceEvent());

                if (maxChangeOfMachines >= 0) {
                    //Adding the machinesKilled + maxChangeOfMachines to be spawned: because Assumption1
                    int machinesToBeAdded = maxChangeOfMachines + killedInstances;
                    boolean inCoolDownPeriod = isInCoolDownPeriod(event.getGroupId(), ScalingSuggestion.ScalingDirection.SCALE_OUT);

                    if (!inCoolDownPeriod) {
                        scaleOutInternalQueue.add(event.getGroupId().concat(":").concat(String.valueOf(machinesToBeAdded)));
                        machineChanges = machinesToBeAdded;
                    }
                } else {
                    int machinesToBeRemoved = Math.abs(maxChangeOfMachines);
                    if (machinesToBeRemoved > killedInstances) {
                        if (!isInCoolDownPeriod(event.getGroupId(), ScalingSuggestion.ScalingDirection.SCALE_IN)) {
                            machinesToBeRemoved = machinesToBeRemoved - killedInstances;
                            if (machinesToBeRemoved >= endOfBillingMachineIds.size()) {
                                //kill all machines at end of billing period. (we won't kill machines which are not at the
                                //end of billing period to utilize the already payed machines
                                addScaleInSuggestions(event.getGroupId(), endOfBillingMachineIds);
                                machineChanges = (-1) * endOfBillingMachineIds.size();

                                /*won 't add to internal queue and wait for queue in while loop until there is a real need
                                scaleInInternalQueue.addAll(endOfBillingMachineIds);  */
                            } else {
                                //kill number of machinesToBeRemoved selected from endOfBilling set
                                ArrayList<String> toRemoveList = new ArrayList<String>(endOfBillingMachineIds.subList(0, machinesToBeRemoved));
                                addScaleInSuggestions(event.getGroupId(), toRemoveList);
                                machineChanges = (-1) * toRemoveList.size();
                            }
                        } //will not scale in since in cooldown period
                    } else {
                        int machinesToBeAdded = killedInstances - machinesToBeRemoved;
                        boolean inCoolDownPeriod = isInCoolDownPeriod(event.getGroupId(), ScalingSuggestion.ScalingDirection.SCALE_OUT);
                        if (!inCoolDownPeriod) {
                            scaleOutInternalQueue.add(event.getGroupId().concat(":").concat(String.valueOf(machinesToBeAdded)));
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
        private int handleWithAssumption2(ArrayList<String> endOfBillingMachineIds, ProfiledMachineEvent event) throws ElasticScalarException {
            ProfiledResourceEvent resourceEventOfGroup = event.getProfiledResourceEvent();
            RuntimeGroupInfo runtimeGroupInfo = activeGroupsInfo.get(event.getGroupId());
            int machineChanges = 0;

            if (resourceEventOfGroup != null) {
                int maxChangeOfMachines = getNumberOfMachineChanges(event.getProfiledResourceEvent());

                if (maxChangeOfMachines >= 0) {
                    //adding only the maxChangeOfMachines to be spawned. because: Assumption2
                    boolean inCoolDownPeriod = isInCoolDownPeriod(event.getGroupId(), ScalingSuggestion.ScalingDirection.SCALE_OUT);

                    if (!inCoolDownPeriod) {
                        scaleOutInternalQueue.add(event.getGroupId().concat(":").concat(String.valueOf(maxChangeOfMachines)));
                        machineChanges = maxChangeOfMachines;
                    }

                } else {
                    int machinesToBeRemoved = Math.abs(maxChangeOfMachines);
                    if (!isInCoolDownPeriod(event.getGroupId(), ScalingSuggestion.ScalingDirection.SCALE_IN)) {
                        if (machinesToBeRemoved >= endOfBillingMachineIds.size()) {
                            //kill all machines at end of billing period. (we won't kill machines which are not at the
                            //end of billing period to utilize the already payed machines
                            addScaleInSuggestions(event.getGroupId(), endOfBillingMachineIds);
                            machineChanges = (-1) * endOfBillingMachineIds.size();
                            //runtimeGroupInfo.setScaleInInfo(endOfBillingMachineIds.size());

                        /* won't add to internal queue and wait for queue in while loop until there is a real need
                                    scaleInInternalQueue.addAll(endOfBillingMachineIds); */
                        } else {
                            //kill number of machinesToBeRemoved selected from endOfBilling set
                            ArrayList<String> toRemoveList = new ArrayList<String>(endOfBillingMachineIds.subList(0, machinesToBeRemoved));
                            addScaleInSuggestions(event.getGroupId(), toRemoveList);
                            machineChanges = (-1) * toRemoveList.size();

                            //runtimeGroupInfo.setScaleInInfo(toRemoveList.size());
                        /* won't add to internal queue and wait for queue in while loop until there is a real need
                            scaleInInternalQueue.addAll(endOfBillingMachineIds);
                            scaleInInternalQueue.add(new ArrayList<String>(endOfBillingMachineIds.subList(0, machinesToBeRemoved)));*/
                        }
                    }
                }
            }//else do nothing. Since no resource events: load not high or low
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

    private boolean isInCoolDownPeriod(String groupId, ScalingSuggestion.ScalingDirection direction) throws ElasticScalarException {
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
                } catch (ElasticScalarException e) {
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

    private class ScaleInDecisionMaker implements Runnable {

        MachineProposer machineProposer = new KaramelMachineProposer();  //TODO: get 'which proposer to use' from a config file

        public void run() {
            String groupId = "";
            int noOfMachines;
            Group group;
            while (true) {
                try {
                    String suggestion = scaleInInternalQueue.take(); //suggestion is in the form <groupId>:<noOfMachines>
                    groupId = suggestion.substring(0,suggestion.lastIndexOf(":"));
                    noOfMachines = Integer.parseInt(suggestion.substring(suggestion.lastIndexOf(":") + 1, suggestion.length()));

                   /* group = groupManager.getGroup(groupId);
                    Map<Group.ResourceRequirement, Integer> minResourceReq = group.getMinResourceReq();
                    MachineType[] machineProposals = machineProposer.getMachineProposals(groupId, minResourceReq,
                            noOfMachines, group.getReliabilityReq());
                    addMachinesToSuggestions(groupId, machineProposals);*/

                } catch (InterruptedException e) {
                    log.error("Error while retrieving item from scaleOutInternalQueue. " + e.getMessage());
                }
            }
        }

    }

    public EventProfiler getEventProfiler() {
        return eventProfiler;
    }
}
