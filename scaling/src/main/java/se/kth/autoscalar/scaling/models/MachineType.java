package se.kth.autoscalar.scaling.models;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MachineType {

    String region;
    String instanceType;
    boolean isABiddingInstance;
    float bid;

    public MachineType(String region, String instanceType) {
        this.region = region;
        this.instanceType = instanceType;
    }

    public String getRegion() {
        return region;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public boolean isABiddingInstance() {
        return isABiddingInstance;
    }

    public void setABiddingInstance(boolean ABiddingInstance) {
        isABiddingInstance = ABiddingInstance;
    }

    public float getBid() {
        return bid;
    }

    public void setBid(float bid) {
        this.bid = bid;
    }

}
