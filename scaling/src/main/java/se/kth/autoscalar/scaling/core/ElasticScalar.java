package se.kth.autoscalar.scaling.core;

import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public interface ElasticScalar {

    boolean startElasticScaling(String groupId);

    boolean stopElasticScaling(String groupId);

    //return a reference to the queue where elastic scaling decision are being added
    Queue getSuggestionQueue();
}
