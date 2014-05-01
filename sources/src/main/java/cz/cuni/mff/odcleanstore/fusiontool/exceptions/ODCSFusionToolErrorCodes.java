package cz.cuni.mff.odcleanstore.fusiontool.exceptions;

/**
 * Error codes used in ODCS-FusionTool.
 * The purpose of error codes is to identify the place in code where an error occurred.
 *
 * @author Jan Michelfeit
 */
public final class ODCSFusionToolErrorCodes {

    /** Disable constructor for a utility class. */
    private ODCSFusionToolErrorCodes() {
    }

    // CHECKSTYLE:OFF
    public static final int QUERY_NG_METADATA = 10;

    public static final int QUERY_SAMEAS = 11;

    public static final int QUERY_TRIPLE_SUBJECTS = 12;
    public static final int TRIPLE_SUBJECT_ITERATION = 13;
    public static final int QUERY_QUADS = 14;
    public static final int SEED_AND_SOURCE_VARIABLE_CONFLICT = 15;
    //public static final int REPOSITORY_INIT = 16;
    public static final int REPOSITORY_CONFIG = 17;
    public static final int REPOSITORY_UNSUPPORTED = 18;
    public static final int REPOSITORY_INIT_VIRTUOSO = 19;
    public static final int REPOSITORY_INIT_FILE = 20;
    //public static final int REPOSITORY_ACCESS = 21;
    public static final int REPOSITORY_INIT_SPARQL = 22;
    public static final int REPOSITORY_CLOSE = 23;
    public static final int OUTPUT_UNSUPPORTED = 24;
    public static final int OUTPUT_PARAM = 25;
    //public static final int REPOSITORY_INIT_VIRTUOSO_SPARQL = 26;
    public static final int REPOSITORY_INIT_VIRTUOSO_JDBC = 27;
    public static final int SUBJECTS_SET_LOADER_CLOSE = 28;
    public static final int SUBJECTS_SET_LOADER_INITIAL_SUBJECTS_CLOSE = 29;
}
