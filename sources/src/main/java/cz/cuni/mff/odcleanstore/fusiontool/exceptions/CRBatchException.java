package cz.cuni.mff.odcleanstore.fusiontool.exceptions;

import cz.cuni.mff.odcleanstore.shared.ODCleanStoreException;

/**
 * A general CR-batch exception.
 * @author Jan Michelfeit
 */
public class CRBatchException extends ODCleanStoreException {
    private static final long serialVersionUID = 3420323334894817996L;

    private final Integer errorCode;

    /**
     * Constructs a new exception with the given cause.
     * @param errorCode code of the error
     * @param cause the cause
     */
    public CRBatchException(Integer errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    /**
     * Constructs a new exception with the given message and cause.
     * @param errorCode code of the error
     * @param message the detail message
     * @param cause the cause
     */
    public CRBatchException(Integer errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Constructs a new exception with the given message.
     * @param errorCode code of the error
     * @param message the detail message
     */
    public CRBatchException(Integer errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Return the error code of the error.
     * @see cz.cuni.mff.odcleanstore.shared.ODCSErrorCodes
     * @return error code or null
     */
    public Integer getErrorCode() {
        return errorCode;
    }
    
    @Override
    public String getMessage() {
        return "(" + getErrorCode() + ") " + super.getMessage(); 
    }
}
