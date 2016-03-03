package se.kth.autoscalar.scaling.core;

import se.kth.autoscalar.common.monitoring.MachineMonitoringEvent;
import se.kth.autoscalar.common.monitoring.MonitoringEvent;
import se.kth.autoscalar.common.monitoring.ResourceMonitoringEvent;
import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.ScalingSuggestion;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.group.GroupManager;
import se.kth.autoscalar.scaling.group.GroupManagerImpl;
import se.kth.autoscalar.scaling.models.MachineType;
import se.kth.autoscalar.scaling.models.RuntimeGroupInfo;
import se.kth.autoscalar.scaling.rules.Rule;
import se.kth.autoscalar.scaling.rules.RuleManager;
import se.kth.autoscalar.scaling.rules.RuleManagerImpl;

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

    GroupManager groupManager;
    RuleManager ruleManager;

    private Map<String, RuntimeGroupInfo> activeGroupsInfo = new HashMap<String, RuntimeGroupInfo>();
    private Map<String, ArrayBlockingQueue<ScalingSuggestion>> suggestionMap = new HashMap<String, ArrayBlockingQueue<ScalingSuggestion>>();
    //private ArrayList<String> activeESGroups = new ArrayList<String>();
    //ArrayBlockingQueue<ScalingSuggestion> suggestionsQueue = new ArrayBlockingQueue<ScalingSuggestion>(50);

    public ElasticScalingManager() throws ElasticScalarException {
        ruleManager = RuleManagerImpl.getInstance();
        groupManager = GroupManagerImpl.getInstance();
    }

    public void addGroupForScaling(String groupId) throws ElasticScalarException {
        if (!activeGroupsInfo.containsKey(groupId))
            addGroupForScaling(groupId, 0);
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

    public void handleEvent(String groupId, MonitoringEvent monitoringEvent,
                            RuleSupport.Comparator comparator) throws ElasticScalarException {
        if (monitoringEvent instanceof ResourceMonitoringEvent) {
            handleResourceMonitoringEvent(groupId, (ResourceMonitoringEvent)monitoringEvent, comparator);
        } else if (monitoringEvent instanceof MachineMonitoringEvent) {
            //support later
        }
    }

    private void handleResourceMonitoringEvent(String groupId, ResourceMonitoringEvent event,
                                               RuleSupport.Comparator comparator) throws ElasticScalarException {
        if(activeGroupsInfo.containsKey(groupId)) {
            Rule[] matchingRules = groupManager.getMatchingRulesForGroup(groupId, event.getResourceType(), comparator, event.getCurrentValue());
            //TODO: decide what to do based on rules and cooling time
            int maxChange = 0;

            for (Rule rule : matchingRules) {
                if (maxChange < rule.getOperationAction())
                    maxChange = rule.getOperationAction();
            }

            ScalingSuggestion suggestion;

            if (RuleSupport.Comparator.GREATER_THAN.name().equals(comparator.name()) ||
                    RuleSupport.Comparator.GREATER_THAN_OR_EQUAL.name().equals(comparator.name())) {
                ArrayList<MachineType> suggestions = new ArrayList<MachineType>();
                //TODO add suggestions to array
                 suggestion = new ScalingSuggestion(suggestions.toArray(new MachineType[suggestions.size()]));
            } else {
                ArrayList<String> machineIdsToKill = new ArrayList<String>();
                //TODO add machine Ids
                suggestion = new ScalingSuggestion(machineIdsToKill.toArray(new String[machineIdsToKill.size()]));
            }
            addSuggestion(groupId, suggestion);
        }
    }

    private void addSuggestion (String groupId, ScalingSuggestion suggestion) {

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
