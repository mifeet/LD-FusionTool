package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import java.util.Locale;

import org.openrdf.OpenRDFException;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.fusiontool.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestrictionImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolQueryException;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import cz.cuni.mff.odcleanstore.vocabulary.OWL;

/**
 * Loads URIs and metadata of named graphs to be processed.
 * @author Jan Michelfeit
 */
public class MetadataLoader extends RepositoryLoaderBase {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataLoader.class);
    
    private static final SparqlRestriction DEFAULT_METADATA_RESTRICTION = new SparqlRestrictionImpl(
            "GRAPH ?308ae1cdfa_x { ?308ae1cdfa_s ?308ae1cdfa_p ?308ae1cdfa_o}", "308ae1cdfa_x");
    
    /**
     * SPARQL query that gets metadata about named graphs to be processed.
     * Variable {@link #ngRestrictionVar}  represents the named graph.
     * Result contains variables ?gs, ?gp, ?go representing triples with
     * metadata.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     */
    private static final String METADATA_QUERY_SOURCE_RESTRICTION = "%1$s"
            + "\n CONSTRUCT { ?" + VAR_PREFIX + "gs ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go }"
            + "\n WHERE {"
            + "\n   {"
            + "\n     SELECT DISTINCT (?%3$s AS ?" + VAR_PREFIX + "gs) ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go"
            + "\n     WHERE {"
            + "\n       %2$s"
            + "\n       ?%3$s ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go."
            + "\n     }"
            + "\n   }"
            + "\n   UNION" // TODO
            + "\n   {"
            + "\n     SELECT DISTINCT ?" + VAR_PREFIX + "gs"
            +           " (<" + ODCS.publisherScore + "> AS ?" + VAR_PREFIX + "gp) ?" + VAR_PREFIX + "go"
            + "\n     WHERE {"
            + "\n       %2$s"
            + "\n       ?%3$s <" + ODCS.publishedBy + "> ?" + VAR_PREFIX + "gs."
            + "\n       ?" + VAR_PREFIX + "gs <" + ODCS.publisherScore + "> ?" + VAR_PREFIX + "go."
            + "\n     }"
            + "\n   }"
            + "\n }";
    
    /**
     * SPARQL query that gets metadata from named graphs restricted by metadata graph pattern
     * Variable {@link #ngRestrictionVar}  represents the named graph.
     * Result contains variables {@link #ngRestrictionVar}, ?gp, ?go representing triples with
     * metadata about named graph ?{@link #ngRestrictionVar}.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) metadata named graph restriction pattern
     * (3) metadata named graph restriction variable
     * 
     * Note: Graphs without metadata are included too because at least odcs:metadataGraph value is expected.
     */
    private static final String METADATA_QUERY_METADATA_RESTRICTION = "%1$s"
            + "\n CONSTRUCT { ?" + VAR_PREFIX + "gs ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go }"
            + "\n WHERE {"
            + "\n   %2$s" 
            + "\n   GRAPH ?%3$s {" 
            + "\n     ?" + VAR_PREFIX + "gs ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go"
            + "\n     FILTER (?" + VAR_PREFIX + "gp != <" + OWL.sameAs + ">)"
            + "\n   }"
            + "\n }";

    /**
     * Creates a new instance.
     * @param dataSource an initialized data source
     */
    public MetadataLoader(DataSource dataSource) {
        super(dataSource);
    }

    /**
     * Loads relevant metadata and adds them to the given metadata collection.
     * Metadata are loaded for named graphs containing triples to be processed and metadata graphs.
     * @param metadata named graph metadata where loaded metadata are added
     * @throws ODCSFusionToolException repository error
     * @see DataSource#getMetadataGraphRestriction()
     */
    public void loadNamedGraphsMetadata(Model metadata) throws ODCSFusionToolException {
        long startTime = System.currentTimeMillis();
        String query = "";
        try {
            SparqlRestriction restriction;
            String unformatedQuery;
            if (!ODCSFusionToolUtils.isRestrictionEmpty(dataSource.getMetadataGraphRestriction())) {
                restriction = dataSource.getMetadataGraphRestriction();
                unformatedQuery = METADATA_QUERY_METADATA_RESTRICTION;
            } else if (!ODCSFusionToolUtils.isRestrictionEmpty(dataSource.getNamedGraphRestriction())) {
                restriction = dataSource.getNamedGraphRestriction();
                unformatedQuery = METADATA_QUERY_SOURCE_RESTRICTION;
            } else {
                restriction = DEFAULT_METADATA_RESTRICTION;
                unformatedQuery = METADATA_QUERY_SOURCE_RESTRICTION;
            }
            query = String.format(Locale.ROOT, unformatedQuery,
                    getPrefixDecl(),
                    restriction.getPattern(),
                    restriction.getVar());
            loadMetadataInternal(metadata, query);
        } catch (OpenRDFException e) {
            throw new ODCSFusionToolQueryException(ODCSFusionToolErrorCodes.QUERY_NG_METADATA, query, dataSource.getName(), e);
        }
        
        LOG.debug("ODCS-FusionTool: Metadata loaded from source {} in {} ms", 
                dataSource.getName(), System.currentTimeMillis() - startTime);
    }
    
    private void loadMetadataInternal(Model metadata, String query) throws OpenRDFException {
        long startTime = System.currentTimeMillis();
        RepositoryConnection connection = dataSource.getRepository().getConnection();
        GraphQueryResult resultSet = null;
        try {
            resultSet = connection.prepareGraphQuery(QueryLanguage.SPARQL, query).evaluate();
            LOG.debug("ODCS-FusionTool: Metadata query took {} ms", System.currentTimeMillis() - startTime);
            while (resultSet.hasNext()) {
                Statement statement = resultSet.next();
                metadata.add(statement);
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            connection.close();
        }
    }
}
