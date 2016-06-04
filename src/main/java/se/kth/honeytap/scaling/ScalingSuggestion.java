package se.kth.honeytap.scaling;

import se.kth.honeytap.scaling.models.MachineType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * ScalingSuggestion will be added per group
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ScalingSuggestion {

    public enum ScalingDirection {
        SCALE_IN, SCALE_OUT, TMP_SCALEIN; //TODO remove TMP_SCALEIN usage when vmIds are provided and can decide on which VM to be removed
    }

    private ScalingDirection scalingDirection;
    private ArrayList<MachineType> scaleOutSuggestions = null;   //machine types that should be added
    private ArrayList<String> scaleInSuggestions = null;   //machine IDs that should be removed
    private int scaleInNumber;   //number of machines be removed

    public ScalingSuggestion(MachineType[] scaleOutMachineSuggestions) {
        scalingDirection = ScalingDirection.SCALE_OUT;

        if (scaleOutMachineSuggestions.length == 0)
            scaleOutSuggestions = new ArrayList<MachineType>();
        else {
            List<MachineType> suggestions = Arrays.<MachineType>asList(scaleOutMachineSuggestions);
            scaleOutSuggestions = new ArrayList<MachineType>(suggestions);
        }
    }

    public ScalingSuggestion(String[] machineIdsToBeRemoved) {
        scalingDirection = ScalingDirection.SCALE_IN;

        if (machineIdsToBeRemoved.length == 0)
            scaleInSuggestions = new ArrayList<String>();
        else {
            List<String> suggestions = Arrays.<String>asList(machineIdsToBeRemoved);
            scaleInSuggestions = new ArrayList<String>(suggestions);
        }
    }

    public ScalingSuggestion(int numberToRemove) {
        scalingDirection = ScalingDirection.TMP_SCALEIN;
        scaleInNumber = numberToRemove;
    }

    public void addSuggestionsToScaleOut(MachineType machineType) {
        scaleOutSuggestions.add(machineType);
    }

    public void addMachineIdTobeRemoved(String machineId) {
        scaleInSuggestions.add(machineId);
    }

    public ScalingDirection getScalingDirection() {
        return scalingDirection;
    }

    public ArrayList<MachineType> getScaleOutSuggestions() {
        return scaleOutSuggestions;
    }

    public ArrayList<String> getScaleInSuggestions() {
        return scaleInSuggestions;
    }

    public int getScaleInNumber() {
        return scaleInNumber;
    }
}
