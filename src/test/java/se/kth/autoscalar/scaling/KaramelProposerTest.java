package se.kth.autoscalar.scaling;

import org.junit.Test;
import se.kth.autoscalar.scaling.cost.mgt.KaramelMachineProposer;
import se.kth.autoscalar.scaling.group.Group;
import se.kth.autoscalar.scaling.models.MachineType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class KaramelProposerTest {

  //@Test
  public void getProposalTest() {
    KaramelMachineProposer proposer = new KaramelMachineProposer();
    Map<Group.ResourceRequirement, Integer> minReq = new HashMap<Group.ResourceRequirement, Integer>();
    minReq.put(Group.ResourceRequirement.NUMBER_OF_VCPUS, 2);
    minReq.put(Group.ResourceRequirement.RAM, 4);
    minReq.put(Group.ResourceRequirement.STORAGE, 50);

    MachineType[] machineTypes = proposer.getMachineProposals("mgr", minReq, 1, (float) 70.5);
  }
}
