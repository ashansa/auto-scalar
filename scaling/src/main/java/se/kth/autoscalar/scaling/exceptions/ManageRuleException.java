package se.kth.autoscalar.scaling.exceptions;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ManageRuleException extends ElasticScalarException {

    public ManageRuleException(String msg) {
        super(msg);
    }

    public ManageRuleException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
