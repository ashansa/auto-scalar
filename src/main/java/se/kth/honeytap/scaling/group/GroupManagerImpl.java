package se.kth.honeytap.scaling.group;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.honeytap.scaling.monitoring.RuleSupport;
import se.kth.honeytap.scaling.exceptions.ManageGroupException;
import se.kth.honeytap.scaling.rules.Rule;
import se.kth.honeytap.scaling.rules.RuleManager;
import se.kth.honeytap.scaling.rules.RuleManagerImpl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */

public class GroupManagerImpl implements GroupManager {

    Log log = LogFactory.getLog(GroupManagerImpl.class);

    private static GroupManagerImpl groupManager;
    private GroupDAO groupDAO;
    private RuleManager ruleManager;

    private GroupManagerImpl() {}

    public static GroupManagerImpl getInstance() throws HoneyTapException {
        if(groupManager == null) {
            groupManager  = new GroupManagerImpl();
            groupManager.init();
        }
        return groupManager;
    }

    private void init() throws HoneyTapException {
        groupDAO = GroupDAO.getInstance();
        ruleManager = RuleManagerImpl.getInstance();
    }

    public Group createGroup(String groupName, int minInstances, int maxInstances, int coolingTimeUp, int coolingTimeDown,
                             String[] ruleNames, Map<Group.ResourceRequirement, Integer> minResourceReq, float reliabilityReq) throws HoneyTapException {

        String[] validRules = getValidRules(ruleNames);

        if (validRules.length == 0) {
            throw new HoneyTapException("At least one valid rule name should be provided when creating a scaling group");
        } else {
            try {
                Group group = new Group(groupName, minInstances, maxInstances, coolingTimeUp, coolingTimeDown, validRules, minResourceReq, reliabilityReq);
                groupDAO.createGroup(group);
                log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> coolingTimeUp >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + coolingTimeUp);
                log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> coolingTimeDown >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + coolingTimeDown);
                return group;
            } catch (SQLException e) {
                throw new HoneyTapException("Failed to create the scaling group: " + groupName, e.getCause());
            }
        }
    }

    public boolean isGroupExists(String groupName) throws HoneyTapException {
        try {
            return groupDAO.isGroupExists(groupName);
        } catch (SQLException e) {
            throw new HoneyTapException("Error in checking the existance of scaling group " + groupName, e.getCause());
        }
    }

    public Group getGroup(String groupName) throws HoneyTapException {
        try {
            return groupDAO.getGroup(groupName);
        } catch (SQLException e) {
            throw new HoneyTapException("Error while getting the group: " + groupName, e.getCause());
        } catch (ManageGroupException e) {
            throw new HoneyTapException("Error while getting the group: " + groupName, e.getCause());
        }
    }

    public void addRuleToGroup(String groupName, String ruleName) throws HoneyTapException {
        try {
            groupDAO.addRuleToGroup(groupName, ruleName);
        } catch (SQLException e) {
            throw new HoneyTapException("Error in adding the rule: " + ruleName + " to the group: " + groupName, e.getCause());
        }
    }

    public void updateGroup(String groupName, Group group) throws HoneyTapException {
        try {
            groupDAO.updateGroup(groupName, group);
        } catch (SQLException e) {
            throw new HoneyTapException("Failed to update the group: " + groupName);
        }
    }

    public void removeRuleFromGroup(String groupName, String ruleName) throws HoneyTapException {
        try {
            groupDAO.removeRuleFromGroup(groupName, ruleName);
        } catch (SQLException e) {
            throw new HoneyTapException("Error in deleting the rule: " + ruleName + " from the group: " + groupName, e.getCause());
        }
    }

    public String[] getRulesForGroup(String groupName) throws HoneyTapException {
        try {
            return groupDAO.getRulesForGroup(groupName);
        } catch (SQLException e) {
            throw new HoneyTapException("Error in retrieving the rules for the group " + groupName, e.getCause());
        }
    }

    public Rule[] getMatchingRulesForGroup(String groupName, RuleSupport.ResourceType resourceType,
                                           RuleSupport.Comparator comparator, float currentValue) throws HoneyTapException {
        String[] rulesOfGroup = getRulesForGroup(groupName);
        Rule[] matchingRules = ruleManager.getMatchingRulesForConstraints(rulesOfGroup, resourceType, comparator, currentValue );
        return matchingRules;
    }

    public void deleteGroup(String groupName) throws HoneyTapException {
        try {
            groupDAO.deleteGroup(groupName);
        } catch (SQLException e) {
            throw new HoneyTapException("Error while deleting the elastic scaling group: " + groupName, e.getCause());
        }
    }

    private String[] getValidRules(String[] ruleNames) {

        ArrayList<String> validRuleNames = new ArrayList<String>();

        for (String ruleName : ruleNames) {
            try {
                if(ruleManager.isRuleExists(ruleName)) {
                    validRuleNames.add(ruleName);
                }
            } catch (HoneyTapException e) {
                log.warn("Could not check the existance of the rule: " + ruleName);
            }
        }
        return validRuleNames.toArray(new String[validRuleNames.size()]);
    }

    public void deleteTables() throws SQLException {
        groupDAO.tempMethodDeleteTables();
    }
}
