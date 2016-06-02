package se.kth.honeytap.scaling.cost.mgt;

import se.kth.honeytap.scaling.group.Group;
import se.kth.honeytap.scaling.models.MachineType;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MachineProposerImpl implements MachineProposer {

    ArrayList<CostEffectiveSuggestionProvider> suggestionProviders = new ArrayList<CostEffectiveSuggestionProvider>();

    public MachineProposerImpl() {
        suggestionProviders.add(new AWSSuggestionProvider());
        suggestionProviders.add(new GCSuggestionProvider());
    }

    public MachineType[] getMachineProposals(String groupId, Map<Group.ResourceRequirement, Integer> minimumResourceReq,
                                             int noOfMachines, float reliabilityPercentage) {

        Map<Integer, MachineType> allSuggestions = new TreeMap<Integer, MachineType>();
        for (CostEffectiveSuggestionProvider suggestionProvider : suggestionProviders) {
            //TODO handle different MachineType objects having same price. (will replace the first entry in map)
            allSuggestions.putAll(suggestionProvider.getCostEffectiveSuggestions(minimumResourceReq, noOfMachines, reliabilityPercentage));
        }

        ArrayList<MachineType> suggestions = new ArrayList<MachineType>();
        int count = 0;
        for (Map.Entry<Integer, MachineType> entry : allSuggestions.entrySet()) {
            if (count >= noOfMachines)
                break;
            suggestions.add(entry.getValue());
        }
        return suggestions.toArray(new MachineType[suggestions.size()]);
    }
}
