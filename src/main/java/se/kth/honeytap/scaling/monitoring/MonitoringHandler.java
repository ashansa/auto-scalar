package se.kth.honeytap.scaling.monitoring;

import se.kth.honeytap.scaling.exceptions.HoneyTapException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public interface MonitoringHandler {

  public MonitoringListener addGroupForMonitoring(String groupId, InterestedEvent[] interestedEvents) throws HoneyTapException;

  public void addInterestedEvent(String groupId, InterestedEvent[] events, int timeDuration);

  public void removeGroupFromMonitoring(String groupId);
}
