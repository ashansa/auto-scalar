package se.kth.autoscalar.monitoring.cloud;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */

/**
 * Will check the liveliness of the machine and the uptime
 */
public interface StatusMonitorManager {

   /* boolean isMachineAlive(String VMId);

    boolean isMachineCloseToBillingCycle(String VMId);*/

    //TODO add not alive machines and close to billing period machines to 2 different queues and let outside classes
    //access those queues
}
