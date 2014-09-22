package cz.cuni.mff.odcleanstore.fusiontool.exceptions;

import cz.cuni.mff.odcleanstore.core.ODCleanStoreException;

public class LDFusionToolException extends ODCleanStoreException {
    private static final long serialVersionUID = 3420323334894817996L;

    /**
     * Constructs a new exception with the given cause.
     * @param cause the cause
     */
    public LDFusionToolException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the given message and cause.
     * @param message the detail message
     * @param cause the cause
     */
    public LDFusionToolException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the given message.
     * @param message the detail message
     */
    public LDFusionToolException(String message) {
        super(message);
    }
}
