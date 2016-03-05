package se.kth.autoscalar.scaling.group;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.scaling.exceptions.DBConnectionFailureException;
import se.kth.autoscalar.scaling.utils.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * Handle both the Group table and Group-Rule mapping
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class GroupDAO {

    private static final String GROUP_TABLE = "ScaleGroup";
    private static final String GROUP_NAME_COLUMN = "GROUP_NAME";
    private static final String MIN_INSTANCES_COLUMN = "MIN_INSTANCES";
    private static final String MAX_INSTANCES_COLUMN = "MAX_INSTANCES";
    private static final String COOLING_TIME_UP_COLUMN = "COOLING_TIME_UP";
    private static final String COOLING_TIME_DOWN_COLUMN = "COOLING_TIME_DOWN";

    private static final String GROUP_RULE_MAPPING_TABLE = "Group_Rule";
    private static final String RULE_NAME_COLUMN = "RULE_NAME";

    Log log = LogFactory.getLog(GroupDAO.class);

    private static GroupDAO groupDAO;
    private Connection dbConnection;

    private GroupDAO() {}

    public static GroupDAO getInstance() throws DBConnectionFailureException {
        if(groupDAO == null) {
            try {
                groupDAO = new GroupDAO();
                groupDAO.init();
            } catch (DBConnectionFailureException e) {
                DBConnectionFailureException exception = DBUtil.handleDBConnectionException("RuleDAO initialization failed." , e);
                throw exception;
            }
        }
        return groupDAO;
    }

    private void init() throws DBConnectionFailureException {
        dbConnection = DBUtil.getDBConnection();
        createTableIfNotExists();
    }

    private void createTableIfNotExists() throws DBConnectionFailureException {

        //create group table
        try {
            /*String createTableQuery = "CREATE TABLE IF NOT EXISTS " + tableName + "(" +
                    "RULE_ID VARCHAR(255) NOT NULL, " + "RULE_NAME VARCHAR(50) NOT NULL, " +
                    "RESOURCE_TYPE VARCHAR(50) NOT NULL, " + "COMPARATOR VARCHAR(20) NOT NULL, " +
                    "THRESHOLD FLOAT NOT NULL, " + "ACTION INT NOT NULL" + ")";*/


           /* String q = "CREATE TABLE IF NOT EXISTS "+GROUP_TABLE+"("+GROUP_NAME_COLUMN+" VARCHAR(50) NOT NULL, "+
                    (COOLING_TIME_UP_COLUMN)+" INT NOT NULL, "+COOLING_TIME_DOWN_COLUMN+" INT NOT NULL, "+MAX_INSTANCES_COLUMN
            +" INT NOT NULL, "+MIN_INSTANCES_COLUMN+" INT NOT NULL"+")";

            dbConnection.prepareStatement(q).executeUpdate();*/


            StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
            createTableQuery.append(GROUP_TABLE).append("(").
                    append(GROUP_NAME_COLUMN).append(" VARCHAR(50) NOT NULL, ").
                    append(MIN_INSTANCES_COLUMN).append(" INT NOT NULL, ").
                    append(MAX_INSTANCES_COLUMN).append(" INT NOT NULL, ").
                    append(COOLING_TIME_UP_COLUMN).append(" INT NOT NULL, ").
                    append(COOLING_TIME_DOWN_COLUMN).append(" INT NOT NULL, ").
                    append(" UNIQUE (").append(GROUP_NAME_COLUMN).append(")").
                    append(")");

            dbConnection.prepareStatement(createTableQuery.toString()).executeUpdate();

        } catch (SQLException e) {
            DBConnectionFailureException exception = DBUtil.handleDBConnectionException(
                    "Could not create the table " + GROUP_TABLE, e);
            throw exception;
        }

        //create group-rules mapping table
        try {
            StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
            createTableQuery.append(GROUP_RULE_MAPPING_TABLE).append("(").
                    append(GROUP_NAME_COLUMN).append(" VARCHAR(50) NOT NULL, ").
                    append(RULE_NAME_COLUMN).append(" VARCHAR(255) NOT NULL").
                    append(")");
            dbConnection.prepareStatement(createTableQuery.toString()).executeUpdate();
        } catch (SQLException e) {
            DBConnectionFailureException exception = DBUtil.handleDBConnectionException(
                    "Could not create the table " + GROUP_RULE_MAPPING_TABLE, e);
            throw exception;
        }

    }

    /**
     * At least one rule should be attached to the group when a group is created
     * This method will create group as well as group-id mapping
     * @param group
     * @return
     */
    public boolean createGroup(Group group) throws SQLException {

        try {
            dbConnection.setAutoCommit(false);

            //group info and group-rule mapping will be managed through two different tables

            //adding group entry
            String insertGroupQuery = "insert into " + GROUP_TABLE + " VALUES (?,?,?,?,?)";
            PreparedStatement createGrouptatement = dbConnection.prepareStatement(insertGroupQuery);
            createGrouptatement.setString(1, group.getGroupName());
            createGrouptatement.setInt(2, group.getMinInstances());
            createGrouptatement.setInt(3, group.getMaxInstances());
            createGrouptatement.setInt(4, group.getCoolingTimeUp());
            createGrouptatement.setInt(5, group.getCoolingTimeDown());
            createGrouptatement.executeUpdate();

            for (String ruleName : group.getRuleNames()) {
                addGroupRuleMapping(group.getGroupName(), ruleName);
            }

            dbConnection.commit();
            dbConnection.setAutoCommit(true);
            return true;
        } catch (SQLException e) {
            log.error("Could not create the group with name " + group.getGroupName());
            throw e;
        }

    }

    public void addRuleToGroup(String groupName, String ruleName) throws SQLException {
        addGroupRuleMapping(groupName, ruleName);
    }

    public void removeRuleFromGroup(String groupName, String ruleName) throws SQLException {
        StringBuilder deleteMappingQuery = new StringBuilder("DELETE FROM ");
        deleteMappingQuery.append(GROUP_RULE_MAPPING_TABLE).append(" WHERE ").append(GROUP_NAME_COLUMN).
                append(" = ? and ").append(RULE_NAME_COLUMN).append(" = ?");

        PreparedStatement deleteMappingtatement = dbConnection.prepareStatement(deleteMappingQuery.toString());
        deleteMappingtatement.setString(1, groupName);
        deleteMappingtatement.setString(2, ruleName);
        deleteMappingtatement.executeUpdate();
    }

    public boolean isGroupExists(String groupName) throws SQLException {
        try {
            ResultSet resultSet = retrieveGroup(groupName);
            return resultSet.next();
        } catch (SQLException e) {
            throw e;
        }
    }

    public Group getGroup(String groupName) throws SQLException {

        ResultSet resultSet = retrieveGroup(groupName);

        try {
            while (resultSet.next()) {
                //get only the first result since there can be only one result for a given groupName

                groupName = resultSet.getString(GROUP_NAME_COLUMN);
                int minInstances = resultSet.getInt(MIN_INSTANCES_COLUMN);
                int maxInstances = resultSet.getInt(MAX_INSTANCES_COLUMN);
                int coolingTimeUp = resultSet.getInt(COOLING_TIME_UP_COLUMN);
                int coolingTimeDown = resultSet.getInt(COOLING_TIME_DOWN_COLUMN);

                String[] rulesOfGroup = getRuleNamesForGroup(groupName);

                //TODO config db to have new parameters
                return new Group(groupName, minInstances, maxInstances, coolingTimeUp, coolingTimeDown, rulesOfGroup,
                        getDefaultMinResourceReq(), 2.0f);
            }
        } catch (SQLException e) {
            log.error("Error while retrieving the attributes for group " + groupName);
            throw e;
        }
        return null;
    }

    private Map<Group.ResourceRequirement, Integer> getDefaultMinResourceReq() {
        Map<Group.ResourceRequirement, Integer> minReq = new HashMap<Group.ResourceRequirement, Integer>();
        minReq.put(Group.ResourceRequirement.NUMBER_OF_VCPUS, 4);
        minReq.put(Group.ResourceRequirement.RAM, 4);
        minReq.put(Group.ResourceRequirement.STORAGE, 50);
        return minReq;
    }

    /*
    The list of rules in the group will not be updated with this method
     */
    public boolean updateGroup(String groupName, Group group) throws SQLException {

        StringBuilder updateQuery = new StringBuilder("update ");
        updateQuery.append(GROUP_TABLE).append(" set ").
                append(GROUP_NAME_COLUMN).append(" = ?, ").append(MIN_INSTANCES_COLUMN).append(" = ?, ").
                append(MAX_INSTANCES_COLUMN).append(" = ?, ").append(COOLING_TIME_UP_COLUMN).append(" = ?, ").
                append(COOLING_TIME_DOWN_COLUMN).append(" = ? where ").append(GROUP_NAME_COLUMN).append(" = ?");

        try {
            PreparedStatement updateRuleStatement = dbConnection.prepareStatement(updateQuery.toString());
            updateRuleStatement.setString(1, groupName);
            updateRuleStatement.setInt(2, group.getMinInstances());
            updateRuleStatement.setInt(3, group.getMaxInstances());
            updateRuleStatement.setInt(4, group.getCoolingTimeUp());
            updateRuleStatement.setInt(5, group.getCoolingTimeDown());
            updateRuleStatement.setString(6, groupName);
            updateRuleStatement.executeUpdate();
            return true;
        } catch (SQLException e) {
            log.error("Failed to update the group: " + groupName);
            throw e;
        }
    }

    public void deleteGroup(String groupName) throws SQLException {

        try {
            dbConnection.setAutoCommit(false);

            //remove group-rule mappings for the group
            StringBuilder deleteMappingQuery = new StringBuilder("DELETE FROM ");
            deleteMappingQuery.append(GROUP_RULE_MAPPING_TABLE).append(" WHERE ").append(GROUP_NAME_COLUMN).
                    append(" = ?");

            PreparedStatement deleteMappingtatement = dbConnection.prepareStatement(deleteMappingQuery.toString());
            deleteMappingtatement.setString(1, groupName);
            deleteMappingtatement.executeUpdate();

            //remove group entry in group table
            StringBuilder deleteGroupQuery = new StringBuilder("DELETE FROM ");
            deleteGroupQuery.append(GROUP_TABLE).append(" WHERE ").append(GROUP_NAME_COLUMN).append(" = ?");

            PreparedStatement deleteGroupStatement = dbConnection.prepareStatement(deleteGroupQuery.toString());
            deleteGroupStatement.setString(1, groupName);
            deleteGroupStatement.executeUpdate();

            dbConnection.commit();
            dbConnection.setAutoCommit(true);
        } catch (SQLException e) {
            log.error("Failed to delete the group with name: " + groupName);
            throw e;
        }

    }

    public String[] getGroupNamesForRule(String ruleName) throws SQLException {

        StringBuilder selectQuery = new StringBuilder("select * FROM ");
        selectQuery.append(GROUP_RULE_MAPPING_TABLE).append(" where ").append(RULE_NAME_COLUMN).append(" = ?");

        try {
            PreparedStatement selectRuleStatement = dbConnection.prepareStatement(selectQuery.toString());
            selectRuleStatement.setString(1, ruleName);
            ResultSet resultSet = selectRuleStatement.executeQuery();

            ArrayList<String> groupsUsingRule = new ArrayList<String>();
            while (resultSet.next()) {
                groupsUsingRule.add(resultSet.getString(GROUP_NAME_COLUMN));
            }

            return groupsUsingRule.toArray(new String[groupsUsingRule.size()]);
        } catch (SQLException e) {
            log.error("Error occourred while retrieving the groups mappings for rule: " + ruleName);
            throw e;
        }
    }

    public String[] getRulesForGroup(String groupName) throws SQLException {
        StringBuilder selectQuery = new StringBuilder("select * FROM ");
        selectQuery.append(GROUP_RULE_MAPPING_TABLE).append(" where ").append(GROUP_NAME_COLUMN).append(" = ?");

        try {
            PreparedStatement selectRuleStatement = dbConnection.prepareStatement(selectQuery.toString());
            selectRuleStatement.setString(1, groupName);
            ResultSet resultSet = selectRuleStatement.executeQuery();

            ArrayList<String> groupsUsingRule = new ArrayList<String>();
            while (resultSet.next()) {
                groupsUsingRule.add(resultSet.getString(RULE_NAME_COLUMN));
            }

            return groupsUsingRule.toArray(new String[groupsUsingRule.size()]);
        } catch (SQLException e) {
            log.error("Error occourred while retrieving the rules mappings for group: " + groupName);
            throw e;
        }
    }

    private ResultSet retrieveGroup(String groupName) throws SQLException {

        StringBuilder selectQuery = new StringBuilder("select * FROM ");
        selectQuery.append(GROUP_TABLE).append(" where ").append(GROUP_NAME_COLUMN).append(" = ?");

        try {
            PreparedStatement selectRuleStatement = dbConnection.prepareStatement(selectQuery.toString());
            selectRuleStatement.setString(1, groupName);
            ResultSet resultSet = selectRuleStatement.executeQuery(); //there will be only 1 result since groupName is unique
            return resultSet;
        } catch (SQLException e) {
            log.error("Error occourred while retrieving the group with name " + groupName);
            throw e;
        }
    }

    private void addGroupRuleMapping(String groupName, String ruleName) throws SQLException {
        String insertMappingQuery = "insert into " + GROUP_RULE_MAPPING_TABLE + " VALUES (?,?)";
        PreparedStatement createMappingtatement = dbConnection.prepareStatement(insertMappingQuery);
        createMappingtatement.setString(1, groupName);
        createMappingtatement.setString(2, ruleName);
        createMappingtatement.executeUpdate();
    }

    private String[] getRuleNamesForGroup(String groupName) throws SQLException {
        try {
            ResultSet resultSet = retrieveGroupRuleMappingForGroup(groupName);
            ArrayList<String> ruleNames = new ArrayList<String>();

            while (resultSet.next()) {
                ruleNames.add(resultSet.getString(RULE_NAME_COLUMN));
            }

            return ruleNames.toArray(new String[ruleNames.size()]);
        } catch (SQLException e) {
            log.error("Error while retrieving the rule for the group: " + groupName);
            throw e;
        }
    }

    private ResultSet retrieveGroupRuleMappingForGroup(String groupName) throws SQLException {
        StringBuilder selectQuery = new StringBuilder("select * FROM ");
        selectQuery.append(GROUP_RULE_MAPPING_TABLE).append(" where ").append(GROUP_NAME_COLUMN).append(" = ?");

        try {
            PreparedStatement selectRuleStatement = dbConnection.prepareStatement(selectQuery.toString());
            selectRuleStatement.setString(1, groupName);
            ResultSet resultSet = selectRuleStatement.executeQuery();
            return resultSet;
        } catch (SQLException e) {
            log.error("Error occourred while retrieving the rule mappings for group: " + groupName);
            throw e;
        }
    }
}
