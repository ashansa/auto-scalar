package se.kth.honeytap.scaling.group;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.exceptions.ManageGroupException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class Group {

    Log log = LogFactory.getLog(Group.class);

    private String groupName;
    private int minInstances;
    private int maxInstances;  //TODO may be this should he the max cost at a given time (cost of many SIs is less than one onDemand)
    //TODO: int billingPeriod; // in minutes
    private ArrayList<String> ruleNames;

    //minimum time interval in seconds between two scale up actions
    private int coolingTimeOut;

    //minimum time interval in seconds between two scale down actions
    private int coolingTimeIn;

    //minimum resource request per machine minCPU: 4, minRAM: 8GB, minStorage: 100GB ==> all these may not be
    //considered in scaling decision, but when providing machines, we can consider all of them */
    private Map<ResourceRequirement, Integer> minResourceReq;

    private float reliabilityReq;

    public Group(String name, int minInstances, int maxInstances, int coolingTimeOut, int coolingTimeIn,
                 String[] ruleNames, Map<ResourceRequirement, Integer> minResourceReq, float reliabilityReq) throws ManageGroupException {
        this.groupName = name;
        this.minInstances = minInstances;
        this.maxInstances = maxInstances;
        this.coolingTimeOut = coolingTimeOut;
        this.coolingTimeIn = coolingTimeIn;

        if(ruleNames != null && ruleNames.length > 0) {
            this.ruleNames = new ArrayList<String>(Arrays.asList(ruleNames));

        } else {
            this.ruleNames = new ArrayList<String>();
        }

        validateMinResourceReq(minResourceReq);
        this.minResourceReq = minResourceReq;

        this.reliabilityReq = reliabilityReq;
    }

    public String getGroupName() {
        return groupName;
    }

    public int getMinInstances() {
        return minInstances;
    }

    public void setMinInstances(int minInstances) {
        this.minInstances = minInstances;
    }

    public int getMaxInstances() {
        return maxInstances;
    }

    public void setMaxInstances(int maxInstances) {
        this.maxInstances = maxInstances;
    }

    public int getCoolingTimeOut() {
        return coolingTimeOut;
    }

    public void setCoolingTimeOut(int coolingTimeOut) {
        this.coolingTimeOut = coolingTimeOut;
    }

    public int getCoolingTimeIn() {
        return coolingTimeIn;
    }

    public void setCoolingTimeIn(int coolingTimeIn) {
        this.coolingTimeIn = coolingTimeIn;
    }

    public String[] getRuleNames() {
        return ruleNames.toArray(new String[ruleNames.size()]);
    }

    public void addRule(String ruleName) {
        if(!this.ruleNames.contains(ruleName)) {
            this.ruleNames.add(ruleName);
        } else {
             log.info("Rule with name " + ruleName + " already exists in group " + groupName);
        }
    }

    public Map<ResourceRequirement, Integer> getMinResourceReq() {
        return minResourceReq;
    }

    public float getReliabilityReq() {
        return reliabilityReq;
    }

    public enum ResourceRequirement {
        NUMBER_OF_VCPUS, RAM, STORAGE
    }

    private void validateMinResourceReq(Map<ResourceRequirement, Integer> minResourceReq) throws ManageGroupException {
        //should contain all the ResourceRequirements
        if (!(minResourceReq.containsKey(ResourceRequirement.NUMBER_OF_VCPUS) && minResourceReq.containsKey(ResourceRequirement.RAM) &&
                minResourceReq.containsKey(ResourceRequirement.STORAGE))) {
            String msg = "Minimum requirements shouls specify limits for all the required resource types.";
            log.error(msg);
            throw new ManageGroupException(msg);
        }
    }
}
