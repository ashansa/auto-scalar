package se.kth.autoscalar.api;

import se.kth.autoscalar.common.models.MachineInfo;
import se.kth.autoscalar.common.monitoring.RuleSupport.Comparator;
import se.kth.autoscalar.common.monitoring.RuleSupport.ResourceType;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.rules.Rule;

import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class AutoScalarImpl implements AutoScalarAPI {

  public Rule createRule(String ruleName, ResourceType resourceType, Comparator comparator, float thresholdPercentage, int operationAction) throws ElasticScalarException {
    throw new UnsupportedOperationException("#createRule()");
  }

  public Rule getRule(String ruleName) throws ElasticScalarException {
    throw new UnsupportedOperationException("#getRule()");
  }

  public void updateRule(String ruleName, Rule rule) throws ElasticScalarException {
    throw new UnsupportedOperationException("#updateRule()");
  }

  public void deleteRule(String ruleName) throws ElasticScalarException {
    throw new UnsupportedOperationException("#deleteRule()");
  }

  public boolean isRuleExists(String ruleName) throws ElasticScalarException {
    throw new UnsupportedOperationException("#isRuleExists()");
  }

  public boolean isRuleInUse(String ruleName) {
    throw new UnsupportedOperationException("#isRuleInUse()");
  }

  public String[] getRuleUsage(String ruleName) throws ElasticScalarException {
    throw new UnsupportedOperationException("#getRuleUsage()");
  }

  public Group createGroup(String groupName, int minInstances, int maxInstances, int coolingTimeUp, int coolingTimeDown, String[] ruleNames) throws ElasticScalarException {
    throw new UnsupportedOperationException("#createGroup()");
  }

  public boolean isGroupExists(String groupName) throws ElasticScalarException {
    throw new UnsupportedOperationException("#isGroupExists()");
  }

  public Group getGroup(String groupName) throws ElasticScalarException {
    throw new UnsupportedOperationException("#getGroup()");
  }

  public void addRuleToGroup(String groupName, String ruleName) throws ElasticScalarException {
    throw new UnsupportedOperationException("#addRuleToGroup()");
  }

  public void updateGroup(String groupName, Group group) throws ElasticScalarException {
    throw new UnsupportedOperationException("#updateGroup()");
  }

  public void removeRuleFromGroup(String groupName, String ruleName) throws ElasticScalarException {
    throw new UnsupportedOperationException("#removeRuleFromGroup()");
  }

  public void deleteGroup(String groupName) throws ElasticScalarException {
    throw new UnsupportedOperationException("#deleteGroup()");
  }

  public MachineInfo addMachineToGroup(String groupId, String machineId, String sshKeyPath, String IP, int sshPort, String userName) {
    throw new UnsupportedOperationException("#addVMToGroup()");
  }

  public void removeMachineFromGroup(MachineInfo model) {
    throw new UnsupportedOperationException("#removeMachineFromGroup()");
  }

  public boolean startAutoScaling(String groupId) {
    throw new UnsupportedOperationException("#startAutoScaling()");
    //get rules related to group from ES, create RuleBase and start monitoring for group
    //start auto-scaling for group
  }

  public boolean stopAutoScaling(String groupId) {
    throw new UnsupportedOperationException("#stopAutoScaling()");
  }

  public Queue getSuggestionQueue() {
    throw new UnsupportedOperationException("#getSuggestionQueue()");
  }
}
