/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import org.openrdf.model.vocabulary.*;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

/**
 * Global configuration constants.
 * Contains default values and values which cannot be currently set via the configuration file. 
 * @author Jan Michelfeit
 */ 
public final class ConfigConstants {

    /** Disable constructor for a utility class. */
    private ConfigConstants() {
    }

    /**
     * SPARQL query variable name referring to a restricted RDF resource in a constraint pattern.
     * @see SparqlRestriction
     */
    public static final String DEFAULT_RESTRICTION_RESOURCE_VAR = "s";

    /**
     * SPARQL query variable name referring to a restricted named graph in a constraint pattern.
     * @see SparqlRestriction
     */
    public static final String DEFAULT_RESTRICTION_GRAPH_VAR = "g";

    /**
     * Default timeout for database queries in seconds.
     * Zero means no timeout.
     */
    public static final int DEFAULT_QUERY_TIMEOUT = 1200;

    /**
     * Default prefix of named graphs and URIs where query results and metadata in the output are placed.
     */
    public static final String DEFAULT_RESULT_DATA_URI_PREFIX = ODCS.NAMESPACE + "CR/";

    /**
     * Maximum number of values in a generated argument for the "?var IN (...)" SPARQL construct .
     */
    public static final int MAX_QUERY_LIST_LENGTH = 25;

    /**
     * Coefficient used in quality computation formula. Value N means that (N+1)
     * sources with score 1 that agree on the result will increase the result
     * quality to 1.
     */
    public static final double AGREE_COEFFICIENT = 4;

    /**
     * Graph score used if none is given in the input.
     */
    public static final double SCORE_IF_UNKNOWN = 0.5;

    /**
     * Weight of the publisher score.
     */
    public static final double PUBLISHER_SCORE_WEIGHT = 0.2;

    /**
     * Difference between two dates when their distance is equal to MAX_DISTANCE in seconds.
     * 31622400 s ~ 366 days
     */
    public static final long MAX_DATE_DIFFERENCE = 31622400;

    /**
     * Set of default preferred canonical URIs.
     */
    public static final Collection<String> DEFAULT_PREFERRED_CANONICAL_URIS = Arrays.asList(
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
    public static final File WORKING_DIRECTORY = new File(".");

    /**
     * Maximum number of rows to be requested in any SPARQL query.
     * Use Virtuoso Default
     */
    public static final int DEFAULT_SPARQL_RESULT_MAX_ROWS = 100000; // TODO: load from configuration
}

