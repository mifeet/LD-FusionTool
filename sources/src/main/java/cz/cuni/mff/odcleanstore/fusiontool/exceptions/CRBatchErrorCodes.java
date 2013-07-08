package cz.cuni.mff.odcleanstore.fusiontool.exceptions;

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
    public static final int SEED_AND_SOURCE_VARIABLE_CONFLICT = 15;
    public static final int REPOSITORY_INIT = 16;
    public static final int REPOSITORY_CONFIG = 17;
    public static final int REPOSITORY_UNSUPPORTED = 18;
    public static final int REPOSITORY_INIT_VIRTUOSO = 19;
    public static final int REPOSITORY_INIT_FILE = 20;
    public static final int REPOSITORY_ACCESS = 21;
    public static final int REPOSITORY_INIT_SPARQL = 22;
    public static final int REPOSITORY_CLOSE = 23;
    // CHECKSTYLE:ON
}
