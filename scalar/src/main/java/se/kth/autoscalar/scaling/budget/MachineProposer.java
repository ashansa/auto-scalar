package se.kth.autoscalar.scaling.budget;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public interface MachineProposer {

    public enum ResourceRequirement {
        NUMBER_OF_VCPUS, RAM, STORAGE
    }

    MachineType getMachineProposal(Map<ResourceRequirement, Integer> minimumResourceReq, int reliabilityPercentage);

    //to be supported later
    /*MachineType getMachineProposal(Map<ResourceRequirement, Integer> minimumResourceReq, int reliabilityPercentage,
                                   String[] requestedRegions);*/


}
