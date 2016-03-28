package se.kth.autoscalar.scaling;

import se.kth.autoscalar.scaling.models.MachineType;

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
        SCALE_IN, SCALE_OUT, TMP_SCALEIN;
    }

    private ScalingDirection scalingDirection;
    private ArrayList<MachineType> scaleOutSuggestions = null;   //machine types that should be added
    private ArrayList<String> scaleInSuggestions = null;   //machine IDs that should be removed
    private int scaleInNumber;   //number of machines be removed

  /*  public ScalingSuggestion(ScalingDirection direction) {
        scalingDirection = direction;

        if (ScalingDirection.SCALE_IN.equals(scalingDirection)) {
            scaleInSuggestions = new ArrayList<String>();
        } else if (ScalingDirection.SCALE_OUT.equals(scalingDirection)) {
            scaleOutSuggestions = new ArrayList<MachineType>();
        }
    }*/

    public ScalingSuggestion(MachineType[] scaleOutMachineSuggestions) {
        scalingDirection = ScalingDirection.SCALE_OUT;

        if (scaleOutMachineSuggestions.length == 0)
            scaleOutSuggestions = new ArrayList<MachineType>();
        else {
            List<MachineType> suggestions = Arrays.<MachineType>asList(scaleOutMachineSuggestions);
            scaleOutSuggestions = new ArrayList<MachineType>(suggestions);
        }
    }

    public ScalingSuggestion(String[] scaleInSuggestion) {
        scalingDirection = ScalingDirection.SCALE_IN;

        if (scaleInSuggestion.length == 0)
            scaleInSuggestions = new ArrayList<String>();
        else {
            List<String> suggestions = Arrays.<String>asList(scaleInSuggestion);
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
}
