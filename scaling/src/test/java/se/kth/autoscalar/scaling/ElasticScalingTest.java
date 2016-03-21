package se.kth.autoscalar.scaling;

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import se.kth.autoscalar.common.monitoring.ResourceMonitoringEvent;
import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.common.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.core.ElasticScalarAPI;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.rules.Rule;

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
public class ElasticScalingTest {

    private static ElasticScalarAPI elasticScalarAPI;
    private static MonitoringListener monitoringListener;

    private String GROUP_BASE_NAME = "my_group";
    private String RULE_BASE_NAME = "my_rule";
    private int coolingTimeOut = 60;
    private int coolingTimeIn = 300;

    double random;
    static Rule rule1;
    static Rule rule2;
    static Group group;
    String groupName;

    @BeforeClass
    public static void init() throws ElasticScalarException {
        elasticScalarAPI = new ElasticScalarAPI();
    }

    @Test
    public void testElasticScaling() throws ElasticScalarException {

        setBasicRulesNGroup();
        //TODO should set rules in monitoring component
        monitoringListener = elasticScalarAPI.startElasticScaling(group.getGroupName(), 2);
        //TODO pass the listener to monitoring component and it should send events based on rules

        //temporary mocking the monitoring events for scale out 1 machine
        ResourceMonitoringEvent cpuEvent = new ResourceMonitoringEvent(ResourceType.CPU_PERCENTAGE,
                RuleSupport.Comparator.GREATER_THAN, (float) ((random * 100) + 5));
        monitoringListener.onHighCPU(groupName, cpuEvent);
        testCPURules(1, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        //temporary mocking the monitoring events for scale out 2 machine
        ResourceMonitoringEvent ramEvent = new ResourceMonitoringEvent(ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.
                GREATER_THAN_OR_EQUAL, (int)(random * 100) + 3);
        monitoringListener.onHighRam(groupName, ramEvent);

        testRAMRules(2, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        //threshold is lower than rule1, still ask to add 2 instances. So ES should add 2 instances
        Rule ramGreater =  elasticScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 2),
                ResourceType.CPU_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (float) ((random * 100) + 0.5f) , 2);
        elasticScalarAPI.addRuleToGroup(groupName, ramGreater.getRuleName());
        monitoringListener.onHighCPU(groupName, cpuEvent);
        testCPURules(2, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        Rule ramLess =  elasticScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 3),
                ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN, (float) ((random * 10) + 30.5f) , 1);
        Rule ramLessEq =  elasticScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 4),
                ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN_OR_EQUAL, (float) ((random * 10) + 10f) , 2);
        elasticScalarAPI.addRuleToGroup(groupName, ramLess.getRuleName());
        elasticScalarAPI.addRuleToGroup(groupName, ramLessEq.getRuleName());
        ramEvent = new ResourceMonitoringEvent(ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.
                LESS_THAN_OR_EQUAL, (float) ((random * 10) + 5));
        monitoringListener.onLowRam(groupName, ramEvent);
        ///////TODO test low end testRAMRules(2, ScalingSuggestion.ScalingDirection.SCALE_IN);

        elasticScalarAPI.deleteGroup(ramGreater.getRuleName());
        elasticScalarAPI.deleteRule(ramGreater.getRuleName());
        elasticScalarAPI.deleteRule(ramLess.getRuleName());
        elasticScalarAPI.deleteRule(ramLessEq.getRuleName());


    }

    private void setBasicRulesNGroup() {

        try {
            random = Math.random();
            groupName = GROUP_BASE_NAME + String.valueOf((int) (random * 10));
            rule1 = elasticScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int) (random * 10)),
                    ResourceType.CPU_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN, (float) (random * 100), 1);
            rule2 = elasticScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 1),
                    ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (float) ((random * 100) + 2) , 2);

            Map<Group.ResourceRequirement, Integer> minReq = new HashMap<Group.ResourceRequirement, Integer>();
            minReq.put(Group.ResourceRequirement.NUMBER_OF_VCPUS, 4);
            minReq.put(Group.ResourceRequirement.RAM, 8);
            minReq.put(Group.ResourceRequirement.STORAGE, 50);

            group = elasticScalarAPI.createGroup(groupName, (int)(random * 10), (int)(random * 100), coolingTimeOut,
                    coolingTimeIn, new String[]{rule1.getRuleName(), rule2.getRuleName()}, minReq, 2.0f);

        } catch (ElasticScalarException e) {
            throw new IllegalStateException(e);
        }

    }

    private void testCPURules(int expectedMachines, ScalingSuggestion.ScalingDirection expectedDirection) {
        ArrayBlockingQueue<ScalingSuggestion>  suggestions = elasticScalarAPI.getSuggestionQueue(groupName);

        int count = 0;
        while (suggestions == null) {
            suggestions = elasticScalarAPI.getSuggestionQueue(groupName);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("CPU_PERCENTAGE:testElasticScaling thread sleep while getting suggestions interrupted.............");
            }
            count++;
            if (count > 20) {
                new AssertionError("CPU_PERCENTAGE:No suggestion received during one minute");
            }
        }
        try {
            ScalingSuggestion suggestion = suggestions.take();
            Assert.assertEquals(expectedMachines, suggestion.getScaleOutSuggestions().size());
            Assert.assertEquals(expectedDirection, suggestion.getScalingDirection());
            System.out.println("CPU_PERCENTAGE: test pass");

            /*switch (suggestion.getScalingDirection()) {
                //TODO validate suggestion
                case SCALE_IN:
                    System.out.println("...........CPU_PERCENTAGE: got scale in suggestion...........");
                    new AssertionError("CPU_PERCENTAGE: Events were to trigger scale out and retrieved a scale in suggestion");
                    break;
                case SCALE_OUT:
                    Assert.assertEquals(expectedMachines, suggestion.getScaleOutSuggestions().size());
                    System.out.println("...........CPU_PERCENTAGE: got scale out suggestion...........");
                    break;
            }*/
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void testRAMRules(int expectedMachines, ScalingSuggestion.ScalingDirection expectedDirection) {
        ArrayBlockingQueue<ScalingSuggestion>  suggestions = elasticScalarAPI.getSuggestionQueue(groupName);

        suggestions = elasticScalarAPI.getSuggestionQueue(groupName);
        int count = 0;

        while (suggestions == null) {
            suggestions = elasticScalarAPI.getSuggestionQueue(groupName);
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                System.out.println("RAM_PERCENTAGE: testElasticScaling thread sleep while getting suggestions interrupted.............");
            }
            count++;
            if (count > 20) {
                new AssertionError("RAM_PERCENTAGE: No suggestion received during one minute");
            }
        }
        try {
            ScalingSuggestion suggestion = suggestions.take();
            Assert.assertEquals(expectedMachines, suggestion.getScaleOutSuggestions().size());
            Assert.assertEquals(expectedDirection, suggestion.getScalingDirection());
            System.out.println("RAM_PERCENTAGE: test pass");
            /*switch (suggestion.getScalingDirection()) {
                //TODO validate suggestion
                case SCALE_IN:
                    Assert.assertEquals(expectedMachines, suggestion.getScaleOutSuggestions().size());
                    Assert.assertEquals(expectedDirection, suggestion.getScaleOutSuggestions());
                    if (ScalingSuggestion.ScalingDirection.SCALE_IN.equals(expectedDirection)) {
                        System.out.println("RAM_PERCENTAGE: matching decision");
                        Assert.assertEquals(expectedMachines, suggestion.getScaleOutSuggestions().size());
                    } else {
                        new AssertionError("RAM_PERCENTAGE: Received a scale in suggestion, but was expecting " + expectedDirection.name());
                        break;
                    }
                case SCALE_OUT:
                    if (ScalingSuggestion.ScalingDirection.SCALE_OUT.equals(expectedDirection)) {
                        System.out.println("RAM_PERCENTAGE: matching decision");
                        Assert.assertEquals(expectedMachines, suggestion.getScaleOutSuggestions().size());
                    } else {
                        new AssertionError("RAM_PERCENTAGE: Received a scale out suggestion, but was expecting " + expectedDirection.name());
                        break;
                    }
            }*/
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /* public void setMonitoringInfo() {
        HashMap<String, Number> systemInfo = new HashMap<String, Number>();
        systemInfo.put(ResourceType.CPU_PERCENTAGE.name(), 50.5);
        systemInfo.put(ResourceType.RAM_PERCENTAGE.name(), 85);
        //can add other supported resources similarly in future and elastic scalar should iterate the list and consume
        //ES ==> startES ==> getMonitoringInfo iteratively and set it somewhere (setMonitoringInfo) to use in ES logic

        //TODO should get the VM start time when adding a VM to the ES module, in order to decide when to shut down the machine
    }

    public void testRecommendation() {
        HashMap<String, Number> systemReq = new HashMap<String, Number>();
        systemReq.put("Min_CPUs", 4 );
        systemReq.put("Min_Ram", 8);
        systemReq.put("Min_Storage", 100);
    }*/

    @AfterClass
    public static void cleanup() throws ElasticScalarException {
        //elasticScalarAPI.tempMethodDeleteTables();
        elasticScalarAPI.deleteRule(rule1.getRuleName());
        elasticScalarAPI.deleteRule(rule2.getRuleName());
        //elasticScalarAPI.deleteRule(newRule.getRuleName());
        elasticScalarAPI.deleteGroup(group.getGroupName());
    }
}
