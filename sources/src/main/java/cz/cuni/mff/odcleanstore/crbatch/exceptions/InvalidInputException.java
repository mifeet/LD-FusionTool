/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.exceptions;

/**
 * Exception thrown when an invalid input is given.
 * @author Jan Michelfeit
 */
public class InvalidInputException extends Exception {
    private static final long serialVersionUID = 1551176048380588306L;

    /**
     * Constructs a new exception with the given cause.
     * @param cause the cause
     */
    public InvalidInputException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the given message and cause.
     * @param message the detail message
     * @param cause the cause
     */
    public InvalidInputException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the given message.
     * @param message the detail message
     */
    public InvalidInputException(String message) {
        super(message);
    }
}
