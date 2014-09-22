package cz.cuni.mff.odcleanstore.fusiontool.exceptions;

/**
 * A general ODCS-FusionTool exception.
 * @author Jan Michelfeit
 */
public class LDFusionToolQueryException extends LDFusionToolApplicationException {
    private static final long serialVersionUID = 3420323334894817996L;

    private final String query;

    /**
     * Constructs a new exception with the given cause.
     * @param errorCode code of the error
     * @param query query that caused the error
     * @param sourceName data source name
     * @param cause the cause
     */
    public LDFusionToolQueryException(Integer errorCode, String query, String sourceName, Throwable cause) {
        super(errorCode, "Repository query error for source " + sourceName, cause);
        this.query = query;
    }

    /**
     * Return the error code of the error.
     * @see LDFusionToolErrorCodes
     * @return error code or null
     */
    public String getQuery() {
        return query;
    }

    @Override
    public String getMessage() {
        return super.getMessage() + "\n Query:\n" + getQuery();
    }
}
