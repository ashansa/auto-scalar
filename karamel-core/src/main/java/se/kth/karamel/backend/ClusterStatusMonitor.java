/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.kth.karamel.backend;

import org.apache.log4j.Logger;
import se.kth.karamel.backend.machines.MachinesMonitor;
import se.kth.karamel.backend.running.model.ClusterEntity;
import se.kth.karamel.client.model.json.JsonCluster;
import se.kth.karamel.common.Settings;

/**
 *
 * @author kamal
 */
public class ClusterStatusMonitor implements Runnable {
  private static final Logger logger = Logger.getLogger(ClusterStatusMonitor.class);
  private final JsonCluster definition;
  private final MachinesMonitor machinesMonitor;
  private final ClusterEntity clusterEntity;
  
  public ClusterStatusMonitor(MachinesMonitor machinesMonitor, JsonCluster definition,ClusterEntity runtime) {
    this.definition = definition;
    this.machinesMonitor = machinesMonitor;
    this.clusterEntity = runtime;
  }

  
  @Override
  public void run() {
    logger.info(String.format("Cluster-StatusMonitor started for '%s' d'-'", definition.getName()));
    while(true) {
      if (clusterEntity.isFailed())
        machinesMonitor.pause();
      try {
        Thread.sleep(Settings.CLUSTER_FAILURE_DETECTION_INTERVAL);
      } catch (InterruptedException ex) {
      }
    }
  }
  
}
