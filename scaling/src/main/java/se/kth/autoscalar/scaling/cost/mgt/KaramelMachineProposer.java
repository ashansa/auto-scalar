package se.kth.autoscalar.scaling.cost.mgt;

import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.models.MachineType;

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
        for (int i = 0; i < noOfMachines; ++i) {
            if (i%3 ==0)   //just to have different types to test
                machineTypes[i] = new MachineType("us-west-2", "t1.micro");
            else
                machineTypes[i] = new MachineType("us-west-2", "t1.medium");
        }
        return machineTypes;
    }
}
