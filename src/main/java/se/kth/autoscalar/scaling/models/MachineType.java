package se.kth.autoscalar.scaling.models;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MachineType {

    private boolean isPreemptible;
    private Map<String,String> properties;

    public MachineType(boolean isPreemptible, Map<String,String> properties) {
        this.isPreemptible = isPreemptible;
        this.properties = properties;
    }

    public boolean isPreemptible() {
        return isPreemptible;
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

}
