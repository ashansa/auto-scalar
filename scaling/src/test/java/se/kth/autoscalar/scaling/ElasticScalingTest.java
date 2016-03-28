package se.kth.autoscalar.scaling;

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import se.kth.autoscalar.common.monitoring.MachineMonitoringEvent;
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
import java.util.concurrent.TimeUnit;

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
    private int coolingTimeOut = 1;
    private int coolingTimeIn = 1;

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
    public void testElasticScalingAssumption1() throws ElasticScalarException {
        System.out.println("=============== test with Assumption 1 =============");
        System.out.println("=============== resource events are not affected by killed machines =============");

        setBasicRulesNGroup();
        //TODO should set rules in monitoring component
        monitoringListener = elasticScalarAPI.startElasticScaling(group.getGroupName(), 2);
        //TODO pass the listener to monitoring component and it should send events based on rules

        /*
        Testing with ResourceMonitoringEvent
         */
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
        Rule cpuGreatEq =  elasticScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 2),
                ResourceType.CPU_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (float) ((random * 100) + 0.5f) , 2);
        elasticScalarAPI.addRuleToGroup(groupName, cpuGreatEq.getRuleName());
        monitoringListener.onHighCPU(groupName, cpuEvent);
        testCPURules(2, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        Rule ramLess =  elasticScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 3),
                ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN, (float) ((random * 10) + 30.5f) , -1);
        Rule ramLessEq =  elasticScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 4),
                ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN_OR_EQUAL, (float) ((random * 10) + 10f) , -2);
        elasticScalarAPI.addRuleToGroup(groupName, ramLess.getRuleName());
        elasticScalarAPI.addRuleToGroup(groupName, ramLessEq.getRuleName());
        ramEvent = new ResourceMonitoringEvent(ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.
                LESS_THAN_OR_EQUAL, (float) ((random * 10) + 5));
        monitoringListener.onLowRam(groupName, ramEvent);
        testRAMRules(0, null); //scale in happens only at the end of billing period


        /*
        testing with MachineMonitoringEvent
         */

        try {
            //No suggestions should be proposed since no resource events are in the window since thread is sleeping 5sec
            Thread.sleep(000);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted while waiting before sending the machineMonitoringEvent", e);
        }

        MachineMonitoringEvent endOfBillingEvent = new MachineMonitoringEvent(groupName,"vm1", MachineMonitoringEvent.Status.AT_END_OF_BILLING_PERIOD);
        monitoringListener.onStateChange(groupName, endOfBillingEvent);
        testMachineEvents(0, ScalingSuggestion.ScalingDirection.SCALE_IN, 1050);  //no machine removals since no resource monitoring events to decide on scaling

        monitoringListener.onLowRam(groupName, ramEvent);
        monitoringListener.onStateChange(groupName, endOfBillingEvent);
        testMachineEvents(1, ScalingSuggestion.ScalingDirection.SCALE_IN, 1050);  //Even though rules tell to remove 2 machines, since only 1 machine is at end of billing, will remove only 1 machine

        MachineMonitoringEvent endOfBillingEvent2 = new MachineMonitoringEvent(groupName,"vm2", MachineMonitoringEvent.Status.AT_END_OF_BILLING_PERIOD);
        MachineMonitoringEvent killedEvent = new MachineMonitoringEvent(groupName, "vm3", MachineMonitoringEvent.Status.KILLED);
        monitoringListener.onLowRam(groupName, ramEvent);
        monitoringListener.onStateChange(groupName, endOfBillingEvent);
        monitoringListener.onStateChange(groupName, endOfBillingEvent2);
        monitoringListener.onStateChange(groupName, killedEvent);
        testMachineEvents(1, ScalingSuggestion.ScalingDirection.SCALE_IN, 1050);  //rules say: -2. But since 1 killed and 2 end of billing, only 1 should be removed

        monitoringListener.onLowRam(groupName, ramEvent);
        monitoringListener.onStateChange(groupName, endOfBillingEvent);
        monitoringListener.onStateChange(groupName, endOfBillingEvent2);
        testMachineEvents(2, ScalingSuggestion.ScalingDirection.SCALE_IN, 1050);  //rules say: -2.  2 end of billing, should remove both machines

        monitoringListener.onStateChange(groupName, killedEvent);
        testMachineEvents(1, ScalingSuggestion.ScalingDirection.SCALE_OUT, 1050); // since 1 killed, should scale out 1

        testCoolingTime();

        elasticScalarAPI.deleteGroup(cpuGreatEq.getRuleName());
        elasticScalarAPI.deleteRule(cpuGreatEq.getRuleName());
        elasticScalarAPI.deleteRule(ramLess.getRuleName());
        elasticScalarAPI.deleteRule(ramLessEq.getRuleName());

    }

    //@Test
    public void testElasticScalingAssumption2() throws ElasticScalarException {

        System.out.println("=============== test with Assumption 2 =============");
        System.out.println("=============== resource events are already affected =============");
        setBasicRulesNGroup();
        //TODO should set rules in monitoring component
        monitoringListener = elasticScalarAPI.startElasticScaling(group.getGroupName(), 2);
        //TODO pass the listener to monitoring component and it should send events based on rules

        /*
        Testing with ResourceMonitoringEvent
         */
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
                ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN, (float) ((random * 10) + 30.5f) , -1);
        Rule ramLessEq =  elasticScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 4),
                ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN_OR_EQUAL, (float) ((random * 10) + 10f) , -2);
        elasticScalarAPI.addRuleToGroup(groupName, ramLess.getRuleName());
        elasticScalarAPI.addRuleToGroup(groupName, ramLessEq.getRuleName());
        ramEvent = new ResourceMonitoringEvent(ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.
                LESS_THAN_OR_EQUAL, (float) ((random * 10) + 5));
        monitoringListener.onLowRam(groupName, ramEvent);
        testRAMRules(0, null);    //scale in happens only at the end of billing period


        /*
        testing with MachineMonitoringEvent
         */

        try {
            //No suggestions should be proposed since no resource events are in the window since thread is sleeping 5sec
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted while waiting before sending the machineMonitoringEvent", e);
        }

        MachineMonitoringEvent endOfBillingEvent = new MachineMonitoringEvent(groupName,"vm1", MachineMonitoringEvent.Status.AT_END_OF_BILLING_PERIOD);
        monitoringListener.onStateChange(groupName, endOfBillingEvent);
        testMachineEvents(0, ScalingSuggestion.ScalingDirection.SCALE_IN, 1050);  //no machine removals since no resource monitoring events to decide on scaling

        monitoringListener.onLowRam(groupName, ramEvent);
        monitoringListener.onStateChange(groupName, endOfBillingEvent);
        testMachineEvents(1, ScalingSuggestion.ScalingDirection.SCALE_IN, 1050);  //Even though rules tell to remove 2 machines, since only 1 machine is at end of billing, will remove only 1 machine

        MachineMonitoringEvent endOfBillingEvent2 = new MachineMonitoringEvent(groupName,"vm2", MachineMonitoringEvent.Status.AT_END_OF_BILLING_PERIOD);
        MachineMonitoringEvent killedEvent = new MachineMonitoringEvent(groupName, "vm3", MachineMonitoringEvent.Status.KILLED);
        monitoringListener.onLowRam(groupName, ramEvent);
        monitoringListener.onStateChange(groupName, endOfBillingEvent);
        monitoringListener.onStateChange(groupName, endOfBillingEvent2);
        monitoringListener.onStateChange(groupName, killedEvent);
        testMachineEvents(2, ScalingSuggestion.ScalingDirection.SCALE_IN, 1050);  //rules say: -2. even though 1 machine is
                                // killed, assumption is that the resource events are affected. So remove 2

        monitoringListener.onLowRam(groupName, ramEvent);
        monitoringListener.onStateChange(groupName, endOfBillingEvent);
        monitoringListener.onStateChange(groupName, endOfBillingEvent2);
        testMachineEvents(2, ScalingSuggestion.ScalingDirection.SCALE_IN, 1050);  //rules say: -2.  2 end of billing, should remove both machines

        monitoringListener.onStateChange(groupName, killedEvent);
        testMachineEvents(0, ScalingSuggestion.ScalingDirection.SCALE_OUT, 1050);  // since assumption is the resource events are affected and no
                            // resource events are in this window, won't scale out

        testCoolingTime();

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
        ArrayBlockingQueue<ScalingSuggestion>  suggestionQueue = elasticScalarAPI.getSuggestionQueue(groupName);

        int count = 0;
        while (suggestionQueue == null) {
            suggestionQueue = elasticScalarAPI.getSuggestionQueue(groupName);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("CPU_PERCENTAGE:testElasticScaling thread sleep while getting suggestions interrupted.............");
            }
            count++;
            if (count > 10) {
                new AssertionError("CPU_PERCENTAGE:No suggestion received during one minute");
            }
        }

        ScalingSuggestion suggestion = null;

        if(expectedMachines == 0) {
            int tries = 0;

            while (tries <= 10) {
                try {
                    suggestion = suggestionQueue.poll(1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                tries++;
            }
            Assert.assertNull(suggestion);
            System.out.println("CPU_PERCENTAGE: test pass. Expected machine changes: " + expectedMachines);
            return;
        }

        try {
            suggestion = suggestionQueue.take();
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
        ArrayBlockingQueue<ScalingSuggestion>  suggestionsQueue = elasticScalarAPI.getSuggestionQueue(groupName);
        int count = 0;

        while (suggestionsQueue == null) {
            suggestionsQueue = elasticScalarAPI.getSuggestionQueue(groupName);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("RAM_PERCENTAGE: testElasticScaling thread sleep while getting suggestions interrupted.............");
            }
            count++;
            if (count > 10) {
                new AssertionError("RAM_PERCENTAGE: No suggestion received during one minute");
            }
        }
        ScalingSuggestion suggestion = null;

        if(expectedMachines == 0) {
            int tries = 0;

            while (tries <= 10) {
                try {
                    suggestion = suggestionsQueue.poll(1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                tries++;
            }
            Assert.assertNull(suggestion);
            System.out.println("RAM_PERCENTAGE: test pass. Expected machine changes: " + expectedMachines);
            return;
        }

        try {
            suggestion = suggestionsQueue.take();
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

    private void testMachineEvents(int expectedMachineChanges, ScalingSuggestion.ScalingDirection expectedDirection, int waitingTime) {
        ArrayBlockingQueue<ScalingSuggestion>  suggestionsQueue = elasticScalarAPI.getSuggestionQueue(groupName);

        int count = 0;
        while (suggestionsQueue == null) {
            suggestionsQueue = elasticScalarAPI.getSuggestionQueue(groupName);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println(expectedDirection.name() + " : thread sleep while getting suggestions interrupted.............");
            }
            count++;
            if (count > 10) {
                new AssertionError(expectedDirection.name() + " : No suggestion received during one minute");
            }
        }
        try {
            ScalingSuggestion suggestion = null;

            if(expectedMachineChanges == 0) {
                int tries = 0;

                while (tries <= 20) {
                    suggestion = suggestionsQueue.poll(1000, TimeUnit.MILLISECONDS);
                    tries++;
                }
                Assert.assertNull(suggestion);
                System.out.println("test pass. Expected machine changes: " + expectedMachineChanges);
                return;
            }

            suggestion = suggestionsQueue.take();
            Assert.assertEquals(expectedDirection, suggestion.getScalingDirection());

            if (ScalingSuggestion.ScalingDirection.SCALE_IN.equals(expectedDirection))
                Assert.assertEquals(expectedMachineChanges, suggestion.getScaleInSuggestions().size());
            else
                Assert.assertEquals(expectedMachineChanges, suggestion.getScaleOutSuggestions().size());

            System.out.println(expectedDirection.name() + " : test pass. Expected machine changes: " + expectedMachineChanges);

        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        try {
            //to handle the cooldown period
            Thread.sleep(waitingTime);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void testCoolingTime() throws ElasticScalarException {

        System.out.println("------------ Start: cooling down test ----------");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted while waiting before starting testCoolingTime. ", e);
        }

        Group updatedGroup = group;
        updatedGroup.setCoolingTimeIn(100);
        updatedGroup.setCoolingTimeOut(100);
        elasticScalarAPI.updateGroup(groupName, updatedGroup);

        ResourceMonitoringEvent cpuEvent = new ResourceMonitoringEvent(ResourceType.CPU_PERCENTAGE,
                RuleSupport.Comparator.GREATER_THAN, (float) ((random * 100) + 5));
        monitoringListener.onHighCPU(groupName, cpuEvent);
        testCPURules(0, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        //temporary mocking the monitoring events for scale out 2 machine
        ResourceMonitoringEvent ramEvent = new ResourceMonitoringEvent(ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.
                GREATER_THAN_OR_EQUAL, (int)(random * 100) + 3);
        monitoringListener.onHighRam(groupName, ramEvent);

        testRAMRules(0, null);   //since the coolDown time is higher, wont scale out for event

        System.out.println("------------ Start: cooling down test ----------");
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
