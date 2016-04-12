package se.kth.autoscalar.scaling;

import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import se.kth.autoscalar.scaling.core.AutoScalarAPI;
import se.kth.autoscalar.scaling.exceptions.AutoScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.monitoring.MachineMonitoringEvent;
import se.kth.autoscalar.scaling.monitoring.MonitoringListener;
import se.kth.autoscalar.scaling.monitoring.ResourceMonitoringEvent;
import se.kth.autoscalar.scaling.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.profile.DynamicEventProfiler;
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

    private static AutoScalarAPI autoScalarAPI;
    private static MonitoringListener monitoringListener;

    private String GROUP_BASE_NAME = "my_group";
    private String RULE_BASE_NAME = "my_rule";
    private int coolingTimeOut = 0;
    private int coolingTimeIn = 0;

    double random;
    static Rule rule1;
    static Rule rule2;
    static Group group;
    String groupName;
    String vmId = "vm1";

    static int windowSize;

    @BeforeClass
    public static void init() throws AutoScalarException {
        autoScalarAPI = AutoScalarAPI.getInstance();
        windowSize = DynamicEventProfiler.getWindowSize();
    }

    //@Test
    public void testElasticScalingAssumption1() throws AutoScalarException {
        System.out.println("=============== test with Assumption 1 : affects are not reflected in window =============");
        System.out.println("=============== resource events are not affected by killed machines =============");

        setBasicRulesNGroup();
        //TODO should set rules in monitoring component
        monitoringListener = autoScalarAPI.startAutoScaling(group.getGroupName(), 2);
        //TODO pass the listener to monitoring component and it should send events based on rules

        /*
        Testing with ResourceMonitoringEvent
         */
        //temporary mocking the monitoring events for scale out 1 machine
        ResourceMonitoringEvent cpuEvent = new ResourceMonitoringEvent(groupName, vmId, ResourceType.CPU_PERCENTAGE,
                RuleSupport.Comparator.GREATER_THAN, (float) ((random * 100) + 5));
        monitoringListener.onHighCPU(groupName, cpuEvent);
        testCPURules(1, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        //temporary mocking the monitoring events for scale out 2 machine
        ResourceMonitoringEvent ramEvent = new ResourceMonitoringEvent(groupName, vmId, ResourceType.RAM_PERCENTAGE,
                RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (int)(random * 100) + 3);
        monitoringListener.onHighRam(groupName, ramEvent);

        testRAMRules(2, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        //threshold is lower than rule1, still ask to add 2 instances. So ES should add 2 instances
        Rule cpuGreatEq =  autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 2),
                ResourceType.CPU_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (float) ((random * 100) + 0.5f) , 2);
        autoScalarAPI.addRuleToGroup(groupName, cpuGreatEq.getRuleName());
        monitoringListener.onHighCPU(groupName, cpuEvent);
        testCPURules(2, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        Rule ramLess =  autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 3),
                ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN, (float) ((random * 10) + 30.5f) , -1);
        Rule ramLessEq =  autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 4),
                ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN_OR_EQUAL, (float) ((random * 10) + 10f) , -2);
        autoScalarAPI.addRuleToGroup(groupName, ramLess.getRuleName());
        autoScalarAPI.addRuleToGroup(groupName, ramLessEq.getRuleName());
        ramEvent = new ResourceMonitoringEvent(groupName, vmId, ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.
                LESS_THAN_OR_EQUAL, (float) ((random * 10) + 5));
        monitoringListener.onLowRam(groupName, ramEvent);
        testRAMRules(0, null); //scale in happens only at the end of billing period


        /*
        testing with MachineMonitoringEvent
         */

        try {
            //No suggestions should be proposed since no resource events are in the window since thread sleeps > windowSize
            Thread.sleep(windowSize + 5);
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

        autoScalarAPI.deleteGroup(cpuGreatEq.getRuleName());
        autoScalarAPI.deleteRule(cpuGreatEq.getRuleName());
        autoScalarAPI.deleteRule(ramLess.getRuleName());
        autoScalarAPI.deleteRule(ramLessEq.getRuleName());

    }

    //@Test
    public void testElasticScalingAssumption2() throws AutoScalarException {

        System.out.println("=============== test with Assumption 2 : affects are reflected in window. So can act just based on rules =============");
        System.out.println("=============== resource events are already affected =============");
        setBasicRulesNGroup();
        //TODO should set rules in monitoring component
        monitoringListener = autoScalarAPI.startAutoScaling(group.getGroupName(), 2);
        //TODO pass the listener to monitoring component and it should send events based on rules

        /*
        Testing with ResourceMonitoringEvent
         */
        //temporary mocking the monitoring events for scale out 1 machine
        ResourceMonitoringEvent cpuEvent = new ResourceMonitoringEvent(groupName, vmId, ResourceType.CPU_PERCENTAGE,
                RuleSupport.Comparator.GREATER_THAN, (float) ((random * 100) + 5));
        monitoringListener.onHighCPU(groupName, cpuEvent);
        testCPURules(1, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        //temporary mocking the monitoring events for scale out 2 machine
        ResourceMonitoringEvent ramEvent = new ResourceMonitoringEvent(groupName, vmId, ResourceType.RAM_PERCENTAGE,
                RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (int)(random * 100) + 3);
        monitoringListener.onHighRam(groupName, ramEvent);

        testRAMRules(2, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        //threshold is lower than rule1, still ask to add 2 instances. So ES should add 2 instances
        Rule ramGreater =  autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 2),
                ResourceType.CPU_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (float) ((random * 100) + 0.5f) , 2);
        autoScalarAPI.addRuleToGroup(groupName, ramGreater.getRuleName());
        monitoringListener.onHighCPU(groupName, cpuEvent);
        testCPURules(2, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        Rule ramLess =  autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 3),
                ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN, (float) ((random * 10) + 30.5f) , -1);
        Rule ramLessEq =  autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 4),
                ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.LESS_THAN_OR_EQUAL, (float) ((random * 10) + 10f) , -2);
        autoScalarAPI.addRuleToGroup(groupName, ramLess.getRuleName());
        autoScalarAPI.addRuleToGroup(groupName, ramLessEq.getRuleName());
        ramEvent = new ResourceMonitoringEvent(groupName, vmId, ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.
                LESS_THAN_OR_EQUAL, (float) ((random * 10) + 5));
        monitoringListener.onLowRam(groupName, ramEvent);
        testRAMRules(0, null);    //scale in happens only at the end of billing period


        /*
        testing with MachineMonitoringEvent
         */

        try {
            //No suggestions should be proposed since no resource events are in the window since thread is sleeping > windowSize
            Thread.sleep(windowSize + 5);
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

        autoScalarAPI.deleteGroup(ramGreater.getRuleName());
        autoScalarAPI.deleteRule(ramGreater.getRuleName());
        autoScalarAPI.deleteRule(ramLess.getRuleName());
        autoScalarAPI.deleteRule(ramLessEq.getRuleName());

    }

    private void setBasicRulesNGroup() {

        try {
            random = Math.random();
            groupName = GROUP_BASE_NAME + String.valueOf((int) (random * 10));
            rule1 = autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int) (random * 10)),
                    ResourceType.CPU_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN, (float) (random * 100), 1);
            rule2 = autoScalarAPI.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 1),
                    ResourceType.RAM_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (float) ((random * 100) + 2) , 2);

            Map<Group.ResourceRequirement, Integer> minReq = new HashMap<Group.ResourceRequirement, Integer>();
            minReq.put(Group.ResourceRequirement.NUMBER_OF_VCPUS, 4);
            minReq.put(Group.ResourceRequirement.RAM, 8);
            minReq.put(Group.ResourceRequirement.STORAGE, 50);

            group = autoScalarAPI.createGroup(groupName, (int)(random * 10), (int)(random * 100), coolingTimeOut,
                    coolingTimeIn, new String[]{rule1.getRuleName(), rule2.getRuleName()}, minReq, 2.0f);

        } catch (AutoScalarException e) {
            throw new IllegalStateException(e);
        }

    }

    private void testCPURules(int expectedMachines, ScalingSuggestion.ScalingDirection expectedDirection) {
        ArrayBlockingQueue<ScalingSuggestion>  suggestionQueue = autoScalarAPI.getSuggestionQueue(groupName);

        int count = 0;
        while (suggestionQueue == null) {
            suggestionQueue = autoScalarAPI.getSuggestionQueue(groupName);
            try {
                Thread.sleep(windowSize/10);
            } catch (InterruptedException e) {
                System.out.println("CPU_PERCENTAGE:testElasticScaling thread sleep while getting suggestions interrupted.............");
            }
            count++;
            if (count > 12) {   //waited to cover window size
                //throw new AssertionError("CPU_PERCENTAGE:No suggestion received after window size");
            }
        }

        ScalingSuggestion suggestion = null;

        if(expectedMachines == 0) {
            int tries = 0;

            while (tries <= 12) {
                try {
                    suggestion = suggestionQueue.poll(windowSize/10, TimeUnit.MILLISECONDS);
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

        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void testRAMRules(int expectedMachines, ScalingSuggestion.ScalingDirection expectedDirection) {
        ArrayBlockingQueue<ScalingSuggestion>  suggestionsQueue = autoScalarAPI.getSuggestionQueue(groupName);
        int count = 0;

        while (suggestionsQueue == null) {
            suggestionsQueue = autoScalarAPI.getSuggestionQueue(groupName);
            try {
                Thread.sleep(windowSize/10);
            } catch (InterruptedException e) {
                System.out.println("RAM_PERCENTAGE: testElasticScaling thread sleep while getting suggestions interrupted.............");
            }
            count++;
            if (count > 12) {
                throw new AssertionError("RAM_PERCENTAGE: No suggestion received during one minute");
            }
        }
        ScalingSuggestion suggestion = null;

        if(expectedMachines == 0) {
            int tries = 0;

            while (tries <= 12) {
                try {
                    suggestion = suggestionsQueue.poll(windowSize/10, TimeUnit.MILLISECONDS);
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
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void testMachineEvents(int expectedMachineChanges, ScalingSuggestion.ScalingDirection expectedDirection, int waitingTime) {
        ArrayBlockingQueue<ScalingSuggestion>  suggestionsQueue = autoScalarAPI.getSuggestionQueue(groupName);

        int count = 0;
        while (suggestionsQueue == null) {
            suggestionsQueue = autoScalarAPI.getSuggestionQueue(groupName);
            try {
                Thread.sleep(windowSize/10);
            } catch (InterruptedException e) {
                System.out.println(expectedDirection.name() + " : thread sleep while getting suggestions interrupted.............");
            }
            count++;
            if (count > 12) {
                throw new AssertionError(expectedDirection.name() + " : No suggestion received during one minute");
            }
        }
        try {
            ScalingSuggestion suggestion = null;

            if(expectedMachineChanges == 0) {
                int tries = 0;

                while (tries <= 12) {
                    suggestion = suggestionsQueue.poll(windowSize/10, TimeUnit.MILLISECONDS);
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

    private void testCoolingTime() throws AutoScalarException {

        System.out.println("------------ Start: cooling down test ----------");
        try {
            Thread.sleep(windowSize);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted while waiting before starting testCoolingTime. ", e);
        }

        Group updatedGroup = group;
        updatedGroup.setCoolingTimeIn(100);
        updatedGroup.setCoolingTimeOut(100);
        autoScalarAPI.updateGroup(groupName, updatedGroup);

        ResourceMonitoringEvent cpuEvent = new ResourceMonitoringEvent(groupName, vmId, ResourceType.CPU_PERCENTAGE,
                RuleSupport.Comparator.GREATER_THAN, (float) ((random * 100) + 5));
        monitoringListener.onHighCPU(groupName, cpuEvent);
        testCPURules(0, ScalingSuggestion.ScalingDirection.SCALE_OUT);

        //temporary mocking the monitoring events for scale out 2 machine
        ResourceMonitoringEvent ramEvent = new ResourceMonitoringEvent(groupName, vmId, ResourceType.RAM_PERCENTAGE,
                RuleSupport.Comparator.GREATER_THAN_OR_EQUAL, (int)(random * 100) + 3);
        monitoringListener.onHighRam(groupName, ramEvent);

        testRAMRules(0, null);   //since the coolDown time is higher, wont scale out for event

        System.out.println("------------ Start: cooling down test ----------");
    }

    @AfterClass
    public static void cleanup() throws AutoScalarException {
        autoScalarAPI.deleteRule(rule1.getRuleName());
        autoScalarAPI.deleteRule(rule2.getRuleName());
        autoScalarAPI.deleteGroup(group.getGroupName());
    }
}
