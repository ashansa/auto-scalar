package se.kth.autoscalar.scaling;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.common.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.group.GroupManager;
import se.kth.autoscalar.scaling.group.GroupManagerImpl;
import se.kth.autoscalar.scaling.rules.Rule;
import se.kth.autoscalar.scaling.rules.RuleManager;
import se.kth.autoscalar.scaling.rules.RuleManagerImpl;

import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ElasticScalingTest {

    private static GroupManager groupManager;
    private static RuleManager ruleManager;
    private String GROUP_BASE_NAME = "my_group";
    private String RULE_BASE_NAME = "my_rule";
    private int coolingTimeOut = 60;
    private int coolingTimeIn = 300;

    double random;
    Rule rule;
    Rule rule2;
    Group group;

    @BeforeClass
    public static void init() throws ElasticScalarException {
        groupManager = GroupManagerImpl.getInstance();
        ruleManager = RuleManagerImpl.getInstance();
    }

    public void setMonitoringInfo() {
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
    }

    private void setGroupNRules() {

        try {
            random = Math.random();
            String groupName = GROUP_BASE_NAME + String.valueOf((int) (random * 10));
            rule = ruleManager.createRule(RULE_BASE_NAME + String.valueOf((int) (random * 10)),
                    ResourceType.CPU_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN, (int) (random * 100), 1);
            rule2 = ruleManager.createRule(RULE_BASE_NAME + String.valueOf((int)(random * 10) + 1),
                    ResourceType.CPU_PERCENTAGE, RuleSupport.Comparator.GREATER_THAN, (int)(random * 100), 1);

            group = groupManager.createGroup(groupName, (int)(random * 10),
                    (int)(random * 100), coolingTimeOut, coolingTimeIn, new String[]{rule.getRuleName(), rule2.getRuleName()});

        } catch (ElasticScalarException e) {
            throw new IllegalStateException(e);
        }

    }

    @AfterClass
    public void cleanup() throws ElasticScalarException {
        ruleManager.deleteRule(rule.getRuleName());
        ruleManager.deleteRule(rule2.getRuleName());
        groupManager.deleteGroup(group.getGroupName());
    }
}
