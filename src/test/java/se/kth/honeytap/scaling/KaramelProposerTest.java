package se.kth.honeytap.scaling;

import se.kth.honeytap.scaling.cost.mgt.KaramelMachineProposer;
import se.kth.honeytap.scaling.cost.mgt.MachineProposer;
import se.kth.honeytap.scaling.group.Group;
import se.kth.honeytap.scaling.models.MachineType;

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
    MachineProposer proposer = MachineProposer.getKaramelProposer();
    Map<Group.ResourceRequirement, Integer> minReq = new HashMap<Group.ResourceRequirement, Integer>();
    minReq.put(Group.ResourceRequirement.NUMBER_OF_VCPUS, 2);
    minReq.put(Group.ResourceRequirement.RAM, 4);
    minReq.put(Group.ResourceRequirement.STORAGE, 50);

    MachineType[] machineTypes = proposer.getMachineProposals("mgr", minReq, 1, (float) 70.5);
  }
}
