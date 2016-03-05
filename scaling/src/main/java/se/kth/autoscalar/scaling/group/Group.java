package se.kth.autoscalar.scaling.group;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    private int maxInstances;
    private ArrayList<String> ruleNames;

    //minimum time interval in seconds between two scale up actions
    private int coolingTimeUp;

    //minimum time interval in seconds between two scale down actions
    private int coolingTimeDown;

    //minimum resource request per machine minCPU: 4, minRAM: 8GB, minStorage: 100GB ==> all these may not be
    //considered in scaling decision, but when providing machines, we can consider all of them */
    private Map<ResourceRequirement, Integer> minResourceReq;

    private float reliabilityReq;

    public Group(String name, int minInstances, int maxInstances, int coolingTimeUp, int coolingTimeDown,
                 String[] ruleNames, Map<ResourceRequirement, Integer> minResourceReq, float reliabilityReq) {
        this.groupName = name;
        this.minInstances = minInstances;
        this.maxInstances = maxInstances;
        this.coolingTimeUp = coolingTimeUp;
        this.coolingTimeDown = coolingTimeDown;

        if(ruleNames != null && ruleNames.length > 0) {
            this.ruleNames = new ArrayList<String>(Arrays.asList(ruleNames));

        } else {
            this.ruleNames = new ArrayList<String>();
        }

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

    public int getCoolingTimeUp() {
        return coolingTimeUp;
    }

    public void setCoolingTimeUp(int coolingTimeUp) {
        this.coolingTimeUp = coolingTimeUp;
    }

    public int getCoolingTimeDown() {
        return coolingTimeDown;
    }

    public void setCoolingTimeDown(int coolingTimeDown) {
        this.coolingTimeDown = coolingTimeDown;
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
}
