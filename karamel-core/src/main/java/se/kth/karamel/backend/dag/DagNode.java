/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.karamel.backend.dag;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;
import se.kth.karamel.common.exception.DagConstructionException;
import se.kth.karamel.common.exception.KaramelException;

/**
 *
 * @author kamal
 */
public class DagNode implements DagTaskCallback {

  public static enum Status {

    WAITING, READY, ONGOING, DONE, FAILED;
  }
  private static final Logger logger = Logger.getLogger(DagNode.class);
  private final String id;
  private final Set<DagNode> predecessors = new HashSet<>();
  private final Set<DagNode> successors = new HashSet<>();
  private final Set<DagNode> signaledPredecessor = new HashSet<>();

  private final Set<String> probs = new HashSet<>();
  private DagTask task;
  private Status status = Status.WAITING;
  private int indention = 1;
  
  public DagNode(String id) {
    this.id = id;
  }

  public DagNode(String id, DagTask task) {
    this.id = id;
    this.task = task;
  }

  public String getId() {
    return id;
  }

  public void setTask(DagTask task) throws DagConstructionException {
    if (this.task == null) {
      this.task = task;
    } else {
      throw new DagConstructionException(String.format("An attempt to override the task in the DAG '%s'", task));
    }
  }

  public DagTask getTask() {
    return task;
  }

  public Set<DagNode> getSuccessors() {
    return successors;
  }

  public boolean addSuccessor(DagNode successor) {
    if (!successors.contains(successor)) {
      successors.add(successor);
      successor.addPredecessor(this);
      return true;
    } else {
      return false;
    }
  }

  public Set<DagNode> getPredecessors() {
    return predecessors;
  }

  public void addPredecessor(DagNode predecessor) {
    if (!predecessors.contains(predecessor)) {
      predecessors.add(predecessor);
    }
  }

  public void detectCylcles(String prob, List<DagNode> ancestors) throws DagConstructionException {
    if (ancestors.contains(this)) {
      String message = "Cycle deceted: " + ancestors.toString() + " " + id;
      logger.error(String.format("Prob: %s %s", prob, message));
      throw new DagConstructionException();
    }
    if (probs.contains(prob)) {
//      logger.debug(String.format("Prob: %s has already visited %s", prob, id));
      return;
    } else {
//      logger.debug(String.format("Prob: %s is visiting %s", prob, id));
    }

    probs.add(prob);

    for (DagNode succ : successors) {
      List<DagNode> newList = new ArrayList<>();
      newList.addAll(ancestors);
      newList.add(this);
      succ.detectCylcles(prob, newList);
    }
  }
  
  public void findMaxIndentionLevel(int indention) {
    if (this.indention < indention)
      this.indention = indention;
    for (DagNode newTask : successors) {
      newTask.findMaxIndentionLevel(indention + 1);
    }
  } 

  public String printBfs(String prob, String pref, int indention) {
    if (this.indention > indention || probs.contains(prob)) {
//      logger.debug(String.format("Prob: %s has already visited %s", prob, id));
      return pref + "$" + id + "(" + status.toString() + ")";
    } else {
//      logger.debug(String.format("Prob: %s is visiting %s", prob, id));
    }
    probs.add(prob);

    StringBuilder builder = new StringBuilder();
    builder.append(pref).append("").append(id).append("(").append(status.toString()).append(")");
    for (DagNode newTask : successors) {
      builder.append("\n").append(newTask.printBfs(prob, pref + " " + indention + "|", indention + 1));
    }
    builder.append("\n").append(pref);
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof DagNode && id.hashCode() == o.hashCode());
  }

  @Override
  public String toString() {
    return id;
  }

  @Override
  public void queued() {
    status = Status.READY;
  }

  @Override
  public void started() {
    status = Status.ONGOING;
  }

  public void start() {
    task.submit(this);
  }

  @Override
  public void succeed() {
    logger.debug(String.format("Done '%s'", id));
    status = Status.DONE;
    for (DagNode succ : successors) {
      try {
        succ.signal(this);
      } catch (KaramelException ex) {
        logger.error("", ex);
      }
    }
  }

  public synchronized void signal(DagNode pred) throws KaramelException {
    signaledPredecessor.add(pred);
    if (predecessors.size() == signaledPredecessor.size()) {
      logger.debug(String.format("Submitting '%s'", id));
      if (task == null) {
        String message = String.format("Node is ready to go in the DAG but it doesn't have any task '%s'", id);
        logger.error(message);
        throw new KaramelException(message);
      }

      start();
    }
  }

  @Override
  public void failed(String reason) {
    logger.error(String.format("Failed '%s', DAG is stuck here :(", id));
    status = Status.FAILED;
  }

}
