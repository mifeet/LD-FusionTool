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
    public static final int MISSING_REQUIRED_PARAM = 37;
    public static final int ALL_TRIPLES_QUERY_QUADS = 36;
    public static final int INPUT_LOADER_HAS_NEXT = 35;
    public static final int INPUT_LOADER_LOADING = 34;
    public static final int INPUT_LOADER_PARSE_TEMP_FILE = 33;
    public static final int INPUT_LOADER_SORT = 32;
    public static final int RDF_FILE_LOADER_PARSE = 31;
    public static final int INPUT_LOADER_BUFFER_QUADS = 30;
    public static final int INPUT_LOADER_TMP_FILE_INIT = 29;
    public static final int RDF_FILE_LOADER_READ = 28;
    public static final int REPOSITORY_INIT_VIRTUOSO_JDBC = 27;
    //public static final int REPOSITORY_INIT_VIRTUOSO_SPARQL = 26;
    public static final int OUTPUT_PARAM = 25;
    public static final int OUTPUT_UNSUPPORTED = 24;
    public static final int REPOSITORY_CLOSE = 23;
    public static final int REPOSITORY_INIT_SPARQL = 22;
    //public static final int REPOSITORY_ACCESS = 21;
    public static final int REPOSITORY_INIT_FILE = 20;
    public static final int REPOSITORY_INIT_VIRTUOSO = 19;
    public static final int REPOSITORY_UNSUPPORTED = 18;
    public static final int REPOSITORY_CONFIG = 17;
    //public static final int REPOSITORY_INIT = 16;
    public static final int SEED_AND_SOURCE_VARIABLE_CONFLICT = 15;
    public static final int QUERY_QUADS = 14;
    public static final int TRIPLE_SUBJECT_ITERATION = 13;
    public static final int QUERY_TRIPLE_SUBJECTS = 12;
    public static final int QUERY_SAMEAS = 11;
    public static final int QUERY_NG_METADATA = 10;
}
