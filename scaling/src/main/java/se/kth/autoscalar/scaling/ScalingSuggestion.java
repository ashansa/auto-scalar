package se.kth.autoscalar.scaling;

import se.kth.autoscalar.scaling.models.MachineType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ScalingSuggestion {

    public enum ScalingDirection {
        SCALE_IN, SCALE_OUT;
    }

    ScalingDirection scalingDirection;
    ArrayList<MachineType> scaleOutSuggestions = null;
    ArrayList<String> scaleInSuggestions = null;

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

    public void addSuggestionsToScaleOut(MachineType machineType) {
        scaleOutSuggestions.add(machineType);
    }

    public void addMachineIdTobeRemoved(String machineId) {
        scaleInSuggestions.add(machineId);
    }
}
