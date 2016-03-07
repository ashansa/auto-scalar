package se.kth.autoscalar.exceptions;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class AutoScalarException extends Exception {

    private String message;
    private Throwable cause;


    public AutoScalarException(String msg) {
        this.message = msg;
    }

    public AutoScalarException(String msg, Throwable cause) {
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
