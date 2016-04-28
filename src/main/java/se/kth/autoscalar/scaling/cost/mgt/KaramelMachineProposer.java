package se.kth.autoscalar.scaling.cost.mgt;

import se.kth.autoscalar.scaling.Constants;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.models.MachineType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class KaramelMachineProposer implements MachineProposer {

    public MachineType[] getMachineProposals(String groupId, Map<Group.ResourceRequirement, Integer> minimumResourceReq,
                                                    int noOfMachines, float reliabilityPercentage) {

        //TODO call Kandy and get proposal
        MachineType[] machineTypes = new MachineType[noOfMachines];
        HashMap<String,String> properties = new HashMap<String, String>();
        properties.put(Constants.REGION, "us-west-2");

        for (int i = 0; i < noOfMachines; ++i) {
            if (i%3 ==0) {   //just to have different types to test
                properties.put(Constants.INSTANCE_TYPE, "t2.micro");
                machineTypes[i] = new MachineType("EC2", false, properties);
            } else {
                properties.put(Constants.INSTANCE_TYPE, "t2.medium");
                machineTypes[i] = new MachineType("EC2", false, properties);
            }
        }
        return machineTypes;
    }
}
