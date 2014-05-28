package cz.cuni.mff.odcleanstore.fusiontool.exceptions;

import cz.cuni.mff.odcleanstore.core.ODCleanStoreException;

/**
 * A general ODCS-FusionTool exception.
 */
public class ODCSFusionToolException extends ODCleanStoreException {
    private static final long serialVersionUID = 3420323334894817996L;

    /**
     * Constructs a new exception with the given cause.
     * @param cause the cause
     */
    public ODCSFusionToolException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the given message and cause.
     * @param message the detail message
     * @param cause the cause
     */
    public ODCSFusionToolException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the given message.
     * @param message the detail message
     */
    public ODCSFusionToolException(String message) {
        super(message);
    }
}
