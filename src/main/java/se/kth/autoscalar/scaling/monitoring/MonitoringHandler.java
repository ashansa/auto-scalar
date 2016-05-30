package se.kth.autoscalar.scaling.monitoring;

import se.kth.autoscalar.scaling.exceptions.AutoScalarException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public interface MonitoringHandler {

  public MonitoringListener addGroupForMonitoring(String groupId, InterestedEvent[] interestedEvents) throws AutoScalarException;

  public void addInterestedEvent(String groupId, InterestedEvent[] events, int timeDuration);

  public void removeGroupFromMonitoring(String groupId);
}
