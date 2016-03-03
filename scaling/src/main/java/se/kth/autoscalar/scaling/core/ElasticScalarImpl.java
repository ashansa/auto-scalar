package se.kth.autoscalar.scaling.core;

import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ElasticScalarImpl implements ElasticScalar {

    public boolean startElasticScaling(String groupId) {
        throw new UnsupportedOperationException("#startElasticScaling()");
    }

    public boolean stopElasticScaling(String groupId) {
        throw new UnsupportedOperationException("#stopElasticScaling()");
    }

    public Queue getSuggestionQueue() {
        throw new UnsupportedOperationException("#getSuggestionQueue()");
    }
}
