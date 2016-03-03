package se.kth.autoscalar.scaling.group;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.common.models.MachineInfo;
import se.kth.autoscalar.common.monitoring.RuleSupport;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.rules.Rule;
import se.kth.autoscalar.scaling.rules.RuleManager;
import se.kth.autoscalar.scaling.rules.RuleManagerImpl;

import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */

//TODO move all group related classes to scaling package
public class GroupManagerImpl implements GroupManager {

    Log log = LogFactory.getLog(GroupManagerImpl.class);

    private static GroupManagerImpl groupManager;
    private GroupDAO groupDAO;
    private RuleManager ruleManager;

    private GroupManagerImpl() {}

    public static GroupManagerImpl getInstance() throws ElasticScalarException {
        if(groupManager == null) {
            groupManager  = new GroupManagerImpl();
            groupManager.init();
        }
        return groupManager;
    }

    private void init() throws ElasticScalarException {
        groupDAO = GroupDAO.getInstance();
        ruleManager = RuleManagerImpl.getInstance();
    }

    public Group createGroup(String groupName, int minInstances, int maxInstances, int coolingTimeUp, int coolingTimeDown,
                             String[] ruleNames) throws ElasticScalarException {

        String[] validRules = getValidRules(ruleNames);

        if (validRules.length == 0) {
            throw new ElasticScalarException("At least one valid rule name should be provided when creating a scaling group");
        } else {
            try {
                Group group = new Group(groupName, minInstances, maxInstances, coolingTimeUp, coolingTimeDown, validRules);
                groupDAO.createGroup(group);
                return group;
            } catch (SQLException e) {
                throw new ElasticScalarException("Failed to create the scaling group: " + groupName, e.getCause());
            }
        }
    }

    public boolean isGroupExists(String groupName) throws ElasticScalarException {
        try {
            return groupDAO.isGroupExists(groupName);
        } catch (SQLException e) {
            throw new ElasticScalarException("Error in checking the existance of scaling group " + groupName, e.getCause());
        }
    }

    public Group getGroup(String groupName) throws ElasticScalarException {
        try {
            return groupDAO.getGroup(groupName);
        } catch (SQLException e) {
            throw new ElasticScalarException("Error while getting the group: " + groupName, e.getCause());
        }
    }

    public void addRuleToGroup(String groupName, String ruleName) throws ElasticScalarException {
        try {
            groupDAO.addRuleToGroup(groupName, ruleName);
        } catch (SQLException e) {
            throw new ElasticScalarException("Error in adding the rule: " + ruleName + " to the group: " + groupName, e.getCause());
        }
    }

    public void updateGroup(String groupName, Group group) throws ElasticScalarException {
        try {
            groupDAO.updateGroup(groupName, group);
        } catch (SQLException e) {
            throw new ElasticScalarException("Failed to update the group: " + groupName);
        }
    }

    public void removeRuleFromGroup(String groupName, String ruleName) throws ElasticScalarException {
        try {
            groupDAO.removeRuleFromGroup(groupName, ruleName);
        } catch (SQLException e) {
            throw new ElasticScalarException("Error in deleting the rule: " + ruleName + " from the group: " + groupName, e.getCause());
        }
    }

    public String[] getRulesForGroup(String groupName) throws ElasticScalarException {
        try {
            return groupDAO.getRulesForGroup(groupName);
        } catch (SQLException e) {
            throw new ElasticScalarException("Error in retrieving the rules for the group " + groupName, e.getCause());
        }
    }

    public Rule[] getMatchingRulesForGroup(String groupName, RuleSupport.ResourceType resourceType,
                                           RuleSupport.Comparator comparator, float currentValue) throws ElasticScalarException {
        String[] rulesOfGroup = getRulesForGroup(groupName);
        Rule[] matchingRules = ruleManager.getMatchingRulesForConstraints(rulesOfGroup, resourceType, null, currentValue );
        return matchingRules;
    }

    public void deleteGroup(String groupName) throws ElasticScalarException {
        try {
            groupDAO.deleteGroup(groupName);
        } catch (SQLException e) {
            throw new ElasticScalarException("Error while deleting the elastic scaling group: " + groupName, e.getCause());
        }
    }

    private String[] getValidRules(String[] ruleNames) {

        ArrayList<String> validRuleNames = new ArrayList<String>();

        for (String ruleName : ruleNames) {
            try {
                if(ruleManager.isRuleExists(ruleName)) {
                    validRuleNames.add(ruleName);
                }
            } catch (ElasticScalarException e) {
                log.warn("Could not check the existance of the rule: " + ruleName);
            }
        }
        return validRuleNames.toArray(new String[validRuleNames.size()]);
    }

    //TODO implement
    public void addMachineToGroup(MachineInfo machineInfo) {
        throw new UnsupportedOperationException("#addVMToGroup()");
    }

    //TODO implement
    public void removeMachineFromGroup(MachineInfo model) {
        throw new UnsupportedOperationException("#removeMachineFromGroup()");
    }
}
