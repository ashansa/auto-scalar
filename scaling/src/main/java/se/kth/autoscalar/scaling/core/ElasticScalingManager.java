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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
                    int maxChangeOfMachines = getNumberOfMachineChanges(event);

                    /*if (RuleSupport.Comparator.GREATER_THAN.name().equals(event.getComparator().name()) ||
                            RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name().equals(event.getComparator().name())) {*/
                    if (maxChangeOfMachines > 0) {
                        scaleOutInternalQueue.add(event.getGroupId().concat(":").concat(String.valueOf(maxChangeOfMachines)));
                    }  //no else part
                        // Scale in will trigger only at the end of a billing period of a machine. This is handled by ProfiledMachineEventListener
                        /////scaleInInternalQueue.add(event.getGroupId().concat(":").concat(String.valueOf(maxChangeOfMachines)));
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
        //TODO: decide what to do based on rules and cooling time
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
                    ProfiledResourceEvent resourceEventOfGroup = event.getProfiledResourceEvent();

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

                    if (resourceEventOfGroup != null) {
                        int maxChangeOfMachines = getNumberOfMachineChanges(event.getProfiledResourceEvent());

                        if (maxChangeOfMachines >= 0) {
                            //TODO are we going to add the machinesKilled + maxChangeOfMachines to be spawned
                            //TODO No need to add if the resource events are affected by the machine loss (killed machine it is already reflected)
                            scaleOutInternalQueue.add(event.getGroupId().concat(":").concat(String.valueOf(maxChangeOfMachines)));
                        } else {
                            int machinesToBeRemoved = Math.abs(maxChangeOfMachines);
                            if (machinesToBeRemoved > killedInstances) {
                                machinesToBeRemoved = machinesToBeRemoved - killedInstances;
                                if (machinesToBeRemoved >= endOfBillingMachineIds.size()) {
                                    //kill all machines at end of billing period. (we won't kill machines which are not at the
                                    //end of billing period to utilize the already payed machines
                                    addScaleInSuggestions(event.getGroupId(), endOfBillingMachineIds);

                                    /* won't add to internal queue and wait for queue in while loop until there is a real need
                                    scaleInInternalQueue.addAll(endOfBillingMachineIds); */
                                } else {
                                    //kill number of machinesToBeRemoved selected from endOfBilling set
                                    ArrayList<String> toRemoveList = new ArrayList<String>(endOfBillingMachineIds.subList(0, machinesToBeRemoved));
                                    addScaleInSuggestions(event.getGroupId(), toRemoveList);

                                    /* won't add to internal queue and wait for queue in while loop until there is a real need
                                    scaleInInternalQueue.addAll(endOfBillingMachineIds);
                                        scaleInInternalQueue.add(new ArrayList<String>(endOfBillingMachineIds.subList(0, machinesToBeRemoved)));*/
                                }
                            } // else: we are not spawning machines here since the Profiled resource event say to remove machines
                        }
                    } else {
                        //Since no low resource consumption events are in the window (no resource events), adding machines to replace killed machines
                        scaleOutInternalQueue.add(event.getGroupId().concat(":").concat(String.valueOf(killedInstances)));
                    }
                }  else {
                    throw new ElasticScalarException("Machine event cannot be handled. Group is not in active scaling groups." +
                            " Group Id: " + event.getGroupId());
                }
            }
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
