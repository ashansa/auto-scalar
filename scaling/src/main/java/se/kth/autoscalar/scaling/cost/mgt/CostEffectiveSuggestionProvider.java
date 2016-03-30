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
public interface CostEffectiveSuggestionProvider {

    /**
     *
     * @param minimumResourceReq
     * @param noOfMachines
     * @param reliabilityPercentage
     * @return map of <price,MachineType> suggestions
     */
    public Map<Integer, MachineType> getCostEffectiveSuggestions(Map<Group.ResourceRequirement, Integer> minimumResourceReq,
                                                                 int noOfMachines, float reliabilityPercentage);
}
