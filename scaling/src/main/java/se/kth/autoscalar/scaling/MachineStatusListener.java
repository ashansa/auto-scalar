package se.kth.autoscalar.scaling;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.autoscalar.common.monitoring.MachineMonitoringEvent;
import se.kth.autoscalar.scaling.core.ElasticScalarAPI;
import se.kth.autoscalar.scaling.exceptions.ElasticScalarException;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class MachineStatusListener implements MonitoringListener {

    Log log = LogFactory.getLog(MachineStatusListener.class);

    ElasticScalarAPI elasticScalarAPI;

    public MachineStatusListener(ElasticScalarAPI elasticScalarAPI) throws ElasticScalarException {
        this.elasticScalarAPI = elasticScalarAPI;
    }

    public void onStateChange(String groupId, MachineMonitoringEvent event) throws ElasticScalarException {
        try {
            elasticScalarAPI.handleEvent(groupId, event);
        } catch (ElasticScalarException e) {
            log.error("Error while handling stateChange event for group: " + groupId + " with state change: " +
                    event.getStatus() + " . " + e.getMessage());
            throw e;
        }
    }

}
