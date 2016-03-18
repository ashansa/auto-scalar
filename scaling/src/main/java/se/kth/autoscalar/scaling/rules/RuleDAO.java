package se.kth.autoscalar.scaling.rules;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.common.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.common.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.exceptions.DBConnectionFailureException;
import se.kth.autoscalar.scaling.utils.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class RuleDAO {

    private static final Log log = LogFactory.getLog(RuleDAO.class);

    private static final String RULE_TABLE = "Rule";
    private static final String RULE_NAME_COLUMN = "RULE_NAME";
    private static final String RESOURCE_TYPE_COLUMN = "RESOURCE_TYPE";
    private static final String COMPARATOR_COLUMN = "COMPARATOR";
    private static final String THRESHOLD_COLUMN = "THRESHOLD";
    private static final String ACTION_COLUMN = "ACTION";

    private static RuleDAO ruleDAO;
    private Connection dbConnection;

    public RuleDAO() { }

    public static RuleDAO getInstance() throws DBConnectionFailureException {
        if(ruleDAO == null) {
            try {
                ruleDAO = new RuleDAO();
                ruleDAO.init();
            } catch (DBConnectionFailureException e) {
                DBConnectionFailureException exception = DBUtil.handleDBConnectionException("RuleDAO initialization failed." , e);
                throw exception;
            }
        }
        return ruleDAO;
    }

    private void init() throws DBConnectionFailureException {
        dbConnection = DBUtil.getDBConnection();
        createTableIfNotExists();
    }

    private void createTableIfNotExists() throws DBConnectionFailureException {
        try {
            /*String createTableQuery = "CREATE TABLE IF NOT EXISTS " + tableName + "(" +
                    "RULE_ID VARCHAR(255) NOT NULL, " + "RULE_NAME VARCHAR(50) NOT NULL, " +
                    "RESOURCE_TYPE VARCHAR(50) NOT NULL, " + "COMPARATOR VARCHAR(20) NOT NULL, " +
                    "THRESHOLD FLOAT NOT NULL, " + "ACTION INT NOT NULL" + ")";*/

            StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
            createTableQuery.append(RULE_TABLE).append("(").
                    append(RULE_NAME_COLUMN).append(" VARCHAR(50) NOT NULL, ").
                    append(RESOURCE_TYPE_COLUMN).append(" VARCHAR(50) NOT NULL, ").
                    append(COMPARATOR_COLUMN).append(" VARCHAR(50) NOT NULL, ").
                    append(THRESHOLD_COLUMN).append(" FLOAT NOT NULL, ").
                    append(ACTION_COLUMN).append(" INT NOT NULL,").
                    append(" UNIQUE (").append(RULE_NAME_COLUMN).append(")").
                    append(")");

            dbConnection.prepareStatement(createTableQuery.toString()).executeUpdate();
        } catch (SQLException e) {
            DBConnectionFailureException exception = DBUtil.handleDBConnectionException(
                    "Could not create the table " + RULE_TABLE, e);
            throw exception;
        }
    }

    public boolean createRule(Rule rule) throws SQLException {

        String insertQuery = "insert into " + RULE_TABLE + " VALUES (?,?,?,?,?)";

        try {
            PreparedStatement insertRuleStatement = dbConnection.prepareStatement(insertQuery);
            insertRuleStatement.setString(1, rule.getRuleName());
            insertRuleStatement.setString(2, rule.getResourceType().name());
            insertRuleStatement.setString(3, rule.getComparator().name());
            insertRuleStatement.setFloat(4, rule.getThreshold());
            insertRuleStatement.setInt(5, rule.getOperationAction());
            insertRuleStatement.executeUpdate();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Could not add the rule with name " + rule.getRuleName());
            throw e;
        }
    }

    /**
     * User should check the groups using this rule before changing it to and make sure all groups are OK with new changes
     *
     * @param ruleName
     * @param rule
     */
    public boolean updateRule(String ruleName, Rule rule) throws SQLException {
        /*String updateQuery = "update " + RULE_TABLE + " set RULE_NAME = ?, RESOURCE_TYPE = ?, COMPARATOR = ?, " +
                "THRESHOLD = ?, ACTION = ? where RULE_ID = ?";*/

        StringBuilder updateQuery = new StringBuilder("update ");
        updateQuery.append(RULE_TABLE).append(" set ").
                append(RULE_NAME_COLUMN).append(" = ?, ").append(RESOURCE_TYPE_COLUMN).append(" = ?, ").
                append(COMPARATOR_COLUMN).append(" = ?, ").append(THRESHOLD_COLUMN).append(" = ?, ").
                append(ACTION_COLUMN).append(" = ? where ").append(RULE_NAME_COLUMN).append(" = ?");

        try {
            PreparedStatement updateRuleStatement = dbConnection.prepareStatement(updateQuery.toString());
            updateRuleStatement.setString(1, rule.getRuleName());
            updateRuleStatement.setString(2, rule.getResourceType().name());
            updateRuleStatement.setString(3, rule.getComparator().name());
            updateRuleStatement.setFloat(4, rule.getThreshold());
            updateRuleStatement.setInt(5, rule.getOperationAction());
            updateRuleStatement.setString(6, ruleName);
            updateRuleStatement.executeUpdate();
            return true;
        } catch (SQLException e) {
            log.error("Failed to update the rule with name " + rule.getRuleName());
            throw e;
        }
    }

    /**
     * User should check if the rule is in use before deleting it
     * @param ruleName
     */
    public void deleteRule(String ruleName) throws SQLException {

        StringBuilder deleteQuery = new StringBuilder("DELETE FROM ");
        deleteQuery.append(RULE_TABLE).append(" WHERE ").append(RULE_NAME_COLUMN).append(" = ?");

        try {
            PreparedStatement deleteRuleStatement = dbConnection.prepareStatement(deleteQuery.toString());
            deleteRuleStatement.setString(1, ruleName);
            deleteRuleStatement.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to delete the rule with name " + ruleName);
            throw e;
        }
    }

    public boolean isRuleAlreadyExists(String ruleName) throws SQLException {
        try {
            ResultSet resultSet = retrieveRule(ruleName);
            return resultSet.next();
        } catch (SQLException e) {
            throw e;
        }
    }

    /*public Rule getRuleById(String ruleId) throws SQLException {

        ResultSet resultSet = retrieveRuleWithId(ruleId);

        try {
            while (resultSet.next()) {
                String ruleName = null;
                ruleId = resultSet.getString(RULE_ID_COLUMN);
                ruleName = resultSet.getString(RULE_NAME_COLUMN);
                String resourceType = resultSet.getString(RESOURCE_TYPE_COLUMN);
                String comparator = resultSet.getString(COMPARATOR_COLUMN);
                float threshold = resultSet.getFloat(THRESHOLD_COLUMN);
                int action = resultSet.getInt(ACTION_COLUMN);

                //return the first result since there can be only one result for a given ruleId
                return new Rule(ruleId, ruleName, Rule.ResourceType.valueOf(resourceType),
                        Rule.Comparator.valueOf(comparator), threshold, action);
            }
        } catch (SQLException e) {
            log.error("Error while retrieving the rule attributes for ruleId " + ruleId);
            throw e;
        }
        return null;
    }*/

      /*private ResultSet retrieveRuleWithId(String ruleId) throws SQLException {
//        String selectQuery = "select * FROM " + RULE_TABLE + " where RULE_ID = ?";

        StringBuilder selectQuery = new StringBuilder("select * FROM ");
        selectQuery.append(RULE_TABLE).append(" where ").append(RULE_ID_COLUMN).append(" = ?");

        try {
            PreparedStatement selectRuleStatement = dbConnection.prepareStatement(selectQuery.toString());
            selectRuleStatement.setString(1, ruleId);
            ResultSet resultSet = selectRuleStatement.executeQuery(); //there will be only 1 result since ruleId is unique
            return resultSet;
        } catch (SQLException e) {
            log.error("Error occourred while retrieving the rule with id " + ruleId);
            throw e;
        }
    }*/


    public Rule getRule(String ruleName) throws SQLException {

       ResultSet resultSet = retrieveRule(ruleName);

       try {
           while (resultSet.next()) {
               ruleName = resultSet.getString(RULE_NAME_COLUMN);
               String resourceType = resultSet.getString(RESOURCE_TYPE_COLUMN);
               String comparator = resultSet.getString(COMPARATOR_COLUMN);
               float threshold = resultSet.getFloat(THRESHOLD_COLUMN);
               int action = resultSet.getInt(ACTION_COLUMN);

               //return the first result since there can be only one result for a given ruleName
               return new Rule(ruleName, ResourceType.valueOf(resourceType),
                       Comparator.valueOf(comparator), threshold, action);
           }
       } catch (SQLException e) {
           log.error("Error while retrieving the rule attributes for ruleName " + ruleName);
           throw e;
       }
       return null;
   }

    private ResultSet retrieveRule(String ruleName) throws SQLException {

        StringBuilder selectQuery = new StringBuilder("select * FROM ");
        selectQuery.append(RULE_TABLE).append(" where ").append(RULE_NAME_COLUMN).append(" = ?");

        try {
            PreparedStatement selectRuleStatement = dbConnection.prepareStatement(selectQuery.toString());
            selectRuleStatement.setString(1, ruleName);
            ResultSet resultSet = selectRuleStatement.executeQuery(); //there will be only 1 result since ruleName is unique
            return resultSet;
        } catch (SQLException e) {
            log.error("Error occourred while retrieving the rule with name " + ruleName + " . " + e.getMessage());
            throw e;
        }
    }

    public String[] getRuleUsage(String ruleName) {
        ArrayList<String> groupsUsingRule = new ArrayList<String>();
        //add elements
        return groupsUsingRule.toArray(new String[groupsUsingRule.size()]);
    }

    public void tempMethodDeleteTable() throws SQLException {
        dbConnection.prepareStatement("DROP TABLE IF EXISTS " + RULE_TABLE).executeUpdate();
    }

}
