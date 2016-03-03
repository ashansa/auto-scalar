package se.kth.autoscalar.scaling.exceptions;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class DBConnectionFailureException extends ElasticScalarException {

    public DBConnectionFailureException(String msg) {
        super(msg);
    }

    public DBConnectionFailureException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
