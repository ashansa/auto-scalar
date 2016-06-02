package se.kth.honeytap.scaling.exceptions;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class HoneyTapException extends Exception {

    private String message;
    private Throwable cause;


    public HoneyTapException(String msg) {
        this.message = msg;
    }

    public HoneyTapException(String msg, Throwable cause) {
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
