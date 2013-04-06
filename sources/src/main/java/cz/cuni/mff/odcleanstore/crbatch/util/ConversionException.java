/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.util;

/**
 * Exception thrown when conversion between Jena and Sesame (openrdf) data models fails.
 * @author Jan Michelfeit
 */
public class ConversionException extends Exception {
    private static final long serialVersionUID = 1551176048380588306L;

    /**
     * Constructs a new exception with the given cause.
     * @param cause the cause
     */
    public ConversionException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the given message and cause.
     * @param message the detail message
     * @param cause the cause
     */
    public ConversionException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the given message.
     * @param message the detail message
     */
    public ConversionException(String message) {
        super(message);
    }
}
