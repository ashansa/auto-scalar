package se.kth.honeytap.scaling.monitoring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.Constants;
import se.kth.honeytap.scaling.core.HoneyTapAPI;
import se.kth.honeytap.scaling.exceptions.HoneyTapException;
import se.kth.tablespoon.client.api.MissingParameterException;
import se.kth.tablespoon.client.api.TablespoonApi;
import se.kth.tablespoon.client.events.Comparator;
import se.kth.tablespoon.client.events.EventType;
import se.kth.tablespoon.client.events.Resource;
import se.kth.tablespoon.client.events.ResourceType;
import se.kth.tablespoon.client.events.Threshold;
import se.kth.tablespoon.client.topics.MissingTopicException;
import se.kth.tablespoon.client.topics.ThresholdException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class TSMonitoringHandler implements MonitoringHandler{

    Log log = LogFactory.getLog(TSMonitoringHandler.class);
    private HoneyTapAPI honeyTapAPI;
    private TablespoonApi tablespoonAPI;
    MonitoringListener monitoringListener;
    ArrayList<String> subscribedRegularTopics = new ArrayList<>();

    public TSMonitoringHandler(HoneyTapAPI honeyTapAPI, TablespoonApi tablespoonApi) {
        this.honeyTapAPI = honeyTapAPI;
        this.tablespoonAPI = tablespoonApi;
        this.monitoringListener = new MonitoringListener(this.honeyTapAPI);
    }

    public MonitoringListener addGroupForMonitoring(String groupId, InterestedEvent[] interestedEvents) throws HoneyTapException {


        for (InterestedEvent event : interestedEvents) {
            try {
                //3 forms of items   RAM:<=:30;   CPU:AVG:>=:10:<=:90;    KILLED;
                //but the initial form will be always the 1st form
                String[] items = event.getInterest().split(Constants.SEPARATOR);
                if (items.length != 3) {
                    if (MachineMonitoringEvent.Status.KILLED.name().equals(event.getInterest()) ||
                            MachineMonitoringEvent.Status.AT_END_OF_BILLING_PERIOD.name().equals(event.getInterest())) {

                        log.info("AT_END_OF_BILLING_PERIOD and KILLED events will be handled by Karamel. So will not add to " +
                                "simulator");
                        continue;
                    } else {
                        throw new Exception("Interested event is not in the correct format :" + event.getInterest());
                    }
                }

                Resource resource = new Resource(MonitoringUtil.getMonitoringResourceType(items[0]));
                String uniqueId = tablespoonAPI.submitter().
                        subscriber(monitoringListener).
                        groupId(groupId).
                        eventType(EventType.REGULAR).
                        resource(resource).
                        high((MonitoringUtil.getMonitoringThreshold(items[1], items[2]))).
                        sendRate(1).
                        submit();

                /*if (Comparator.GREATER_THAN_OR_EQUAL.name().equals(MonitoringUtil.
                        getNormalizedComparatorType(Comparator.valueOf(items[1])))) {
                     uniqueId = tablespoonAPI.submitter().
                            subscriber(monitoringListener).
                            groupId(groupId).
                            eventType(EventType.REGULAR).
                            resource(resource).
                            high((MonitoringUtil.getMonitoringThreshold(items[1], items[2]))).
                            sendRate(1).
                            submit();
                } else {
                    uniqueId = tablespoonAPI.submitter().
                            subscriber(monitoringListener).
                            groupId(groupId).
                            eventType(EventType.REGULAR).
                            resource(resource).
                            low((MonitoringUtil.getMonitoringThreshold(items[1], items[2]))).
                            sendRate(1).
                            submit();
                }*/

                subscribedRegularTopics.add(uniqueId);
            } catch (ThresholdException e) {
                log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " + groupId, e);
            } catch (MissingTopicException e) {
                log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " + groupId, e);
            } catch (MissingParameterException e) {
                log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " + groupId, e);
            } catch (IOException e) {
                log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " + groupId, e);
            }  catch (Exception e) {
                log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " +
                        groupId, e);
            }
            // monitoringListener.setUniqueId(uniqueId);

        }

        /*for (InterestedEvent interestedEvent : interestedEvents) {
            //3 forms of items   RAM:<=:30;   CPU:AVG:>=:10:<=:90;    KILLED;
            //but the initial form will be always the 1st form
            ////////
            try {
                String[] items = interestedEvent.getInterest().split(Constants.SEPARATOR);

                tablespoonAPI.createTopic(monitoringListener, groupId, EventType.REGULAR, MonitoringUtil.
                        getMonitoringResourceType(items[0]), 0, 1, MonitoringUtil.getMonitoringThreshold(
                        items[1], items[2]));
            } catch (Exception e) {
                log.warn("Could not add the monitoring event: " + interestedEvent.getInterest() + " for group: " +
                        groupId, e);
            }
        }*/

        return monitoringListener;
    }

    public void removeGroupFromMonitoring(String groupId) {
        // avoid monitoring and sending events of the machines of the group
    }

  /**
   *
   * @param groupId
   * @param events
   * @param timeDuration duration until when the events should be sent in milliseconds
   */
    public void addInterestedEvent(String groupId, InterestedEvent[] events, int timeDuration) {
        //TODO request events for a limited time
        //should handle all interested event types
        //resource interests: CPU:>=:80; RAM:<=:30;   CPU:AVG:>=:10:<=:90  ; CPU:>=:70:<=:80
        // machine interests: KILLED; AT_END_OF_BILLING_PERIOD
        for (InterestedEvent event : events) {
            ////////
            try {
                String items[] = event.getInterest().split(Constants.SEPARATOR);
                if (event.getInterest().contains(Constants.AVERAGE) && items.length == 6 && timeDuration > 0) {  //ie: CPU:AVG:>=:10:<=:90

                    Resource resource = new Resource(MonitoringUtil.getMonitoringResourceType(items[0]));
                    String uniqueId = tablespoonAPI.submitter().
                            subscriber(monitoringListener).
                            groupId(groupId).
                            eventType(EventType.GROUP_AVERAGE).
                            resource(resource).
                            duration(timeDuration/1000).   //timeDuration is in ms
                            sendRate(timeDuration - 1000).   //reduce 1second from duration so the event will be sent only once
                            submit();

                    /*tablespoonAPI.createTopic(monitoringListener, groupId, EventType.GROUP_AVERAGE, MonitoringUtil.
                            getMonitoringResourceType(items[0]), timeDuration, 1, MonitoringUtil.
                            getMonitoringThreshold(items[4], items[5]), MonitoringUtil.getMonitoringThreshold(
                            items[2], items[3]));*/
                } else if (items.length == 5 && timeDuration > 0) {  //ie: CPU:>=:70:<=:80
                    Resource resource = new Resource(MonitoringUtil.getMonitoringResourceType(items[0]));
                    String uniqueId = tablespoonAPI.submitter().
                            subscriber(monitoringListener).
                            groupId(groupId).
                            eventType(EventType.REGULAR).
                            resource(resource).
                            duration(timeDuration/1000).   //timeDuration is in ms
                            sendRate(timeDuration - 1000).   //reduce 1second from duration so the event will be sent only once
                            submit();
                    subscribedRegularTopics.add(uniqueId);
                    /*tablespoonAPI.createTopic(monitoringListener, groupId, EventType.REGULAR, MonitoringUtil.
                            getMonitoringResourceType(items[0]), timeDuration, 1, MonitoringUtil.getMonitoringThreshold(
                            items[3], items[4]), MonitoringUtil.getMonitoringThreshold(items[1], items[2]));*/
                }
            }catch(ThresholdException e){
                log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " + groupId, e);
            } catch (IOException e) {
                log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " + groupId, e);
            } catch (MissingTopicException e) {
                log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " + groupId, e);
            } catch (MissingParameterException e) {
                log.warn("Could not add the monitoring event: " + event.getInterest() + " for group: " + groupId, e);
            }
        }
    }

    public void removeInterestedEvent(String groupId, InterestedEvent events, int timeDuration) {
        //TODO remove events for a limited time
    }
}
