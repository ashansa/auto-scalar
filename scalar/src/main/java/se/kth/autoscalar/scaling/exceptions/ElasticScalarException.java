package se.kth.autoscalar.scaling.exceptions;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class ElasticScalarException extends Exception {

    private String message;
    private Throwable cause;


    public ElasticScalarException(String msg) {
        this.message = msg;
    }

    public ElasticScalarException(String msg, Throwable cause) {
        this.message = msg;
        this.cause = cause;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }
}
