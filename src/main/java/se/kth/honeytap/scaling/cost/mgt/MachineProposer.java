package se.kth.honeytap.scaling.cost.mgt;

import se.kth.honeytap.scaling.group.Group;
import se.kth.honeytap.scaling.models.MachineType;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public abstract class MachineProposer {

    public static MachineProposer getKandyProposer() {
        return new KandyMachineProposer();
    }

    public static MachineProposer getKaramelProposer() {
        return new KandyMachineProposer();
    }

    public abstract MachineType[] getMachineProposals(String groupId, Map<Group.ResourceRequirement, Integer> minimumResourceReq,
                                      int noOfMachines, float reliabilityPercentage);

    //to be supported later
    /*MachineType getMachineProposal(Map<ResourceRequirement, Integer> minimumResourceReq, int reliabilityPercentage,
                                   String[] requestedRegions);*/


}
