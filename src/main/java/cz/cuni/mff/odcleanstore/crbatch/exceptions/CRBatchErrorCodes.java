package cz.cuni.mff.odcleanstore.crbatch.exceptions;

/**
 * Error codes used in CR-Batch.
 * The purpose of error codes is to identify the place in code where an error occurred.
 *
 * @author Jan Michelfeit
 */
public final class CRBatchErrorCodes {
    
    /** Disable constructor for a utility class. */
    private CRBatchErrorCodes() {
    }

    // CHECKSTYLE:OFF
    public static final int QUERY_NG_METADATA = 10;
    public static final int QUERY_SAMEAS = 11;
    public static final int QUERY_TRIPLE_SUBJECTS = 12;
    public static final int TRIPLE_SUBJECT_ITERATION = 13;
    public static final int QUERY_QUADS = 14;
    // CHECKSTYLE:ON
}
