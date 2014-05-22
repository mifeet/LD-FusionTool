/**
 *
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.DC;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.NTriplesParserSettings;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Global configuration constants.
 * Contains default values and values which cannot be currently set via the configuration file.
 * TODO: utilize Spring?
 */
public final class ConfigConstants {
    /** Disable constructor for a utility class. */
    private ConfigConstants() {
    }

    /**
     * SPARQL query variable name referring to a restricted RDF resource in a constraint pattern.
     * @see SparqlRestriction
     */
    static final String DEFAULT_RESTRICTION_RESOURCE_VAR = "s";

    /**
     * SPARQL query variable name referring to a restricted named graph in a constraint pattern.
     * @see SparqlRestriction
     */
    static final String DEFAULT_RESTRICTION_GRAPH_VAR = "g";

    /**
     * Default timeout for database queries in seconds.
     * Zero means no timeout.
     */
    static final int DEFAULT_QUERY_TIMEOUT = 1200;

    /**
     * Default prefix of named graphs and URIs where query results and metadata in the output are placed.
     */
    static final String DEFAULT_RESULT_DATA_URI_PREFIX = ODCS.NAMESPACE + "CR/";

    /**
     * Maximum number of values in a generated argument for the "?var IN (...)" SPARQL construct .
     */
    public static final int MAX_QUERY_LIST_LENGTH = 25;

    /**
     * Coefficient used in quality computation formula. Value N means that (N+1)
     * sources with score 1 that agree on the result will increase the result
     * quality to 1.
     */
    static final double AGREE_COEFFICIENT = 4;

    /**
     * Graph score used if none is given in the input.
     */
    static final double SCORE_IF_UNKNOWN = 0.5;

    /**
     * Weight of the publisher score.
     */
    static final double PUBLISHER_SCORE_WEIGHT = 0.2;

    /**
     * Difference between two dates when their distance is equal to MAX_DISTANCE in seconds.
     * 31622400 s ~ 366 days
     */
    static final long MAX_DATE_DIFFERENCE = 31622400;

    /**
     * Set of default preferred canonical URIs.
     */
    static final Collection<String> DEFAULT_PREFERRED_CANONICAL_URIS = Arrays.asList(
            RDFS.LABEL.stringValue(),
            RDF.TYPE.stringValue(),
            OWL.SAMEAS.stringValue(),
            OWL.NOTHING.stringValue(),
            OWL.THING.stringValue(),
            OWL.CLASS.stringValue(),
            DCTERMS.TITLE.stringValue(),
            DCTERMS.DATE.stringValue(),
            DCTERMS.SOURCE.stringValue(),
            DC.CREATOR.stringValue(),
            DC.SOURCE.stringValue(),
            DC.SUBJECT.stringValue());

    /**
     * Default directory for temporary files.
     */
    static final File DEFAULT_TEMP_DIRECTORY = new File(".");

    /**
     * Maximum number of rows to be requested in any SPARQL query.
     * Use Virtuoso Default
     */
    public static final int DEFAULT_SPARQL_RESULT_MAX_ROWS = 10000;

    /**
     * Max portion of free memory to use.
     */
    static final float MAX_FREE_MEMORY_USAGE = 0.85f;

    /**
     * Set of 'same as' link property URIs for the purposes of conflict resolution.
     */
    static final Set<URI> SAME_AS_LINK_TYPES = new HashSet<URI>(Arrays.asList(
           OWL.SAMEAS
    ));

    /**
     * Default configuration for Sesame file parsers.
     */
    public static final ParserConfig DEFAULT_FILE_PARSER_CONFIG;

    static {
        DEFAULT_FILE_PARSER_CONFIG = new ParserConfig();
        DEFAULT_FILE_PARSER_CONFIG.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
        DEFAULT_FILE_PARSER_CONFIG.set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
        DEFAULT_FILE_PARSER_CONFIG.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        DEFAULT_FILE_PARSER_CONFIG.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
        DEFAULT_FILE_PARSER_CONFIG.addNonFatalError(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
    }

    // TODO: quick constants for vestnik fusion
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
    public static final Collection<URI> RESOURCE_DESCRIPTION_URIS = Arrays.asList(
            VF.createURI("http://schema.org/address"),
            VF.createURI("http://schema.org/contact")
    );


}

