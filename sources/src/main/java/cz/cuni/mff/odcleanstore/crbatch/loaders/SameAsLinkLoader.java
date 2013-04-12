package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.connection.WrappedResultSet;
import cz.cuni.mff.odcleanstore.connection.exceptions.DatabaseException;
import cz.cuni.mff.odcleanstore.connection.exceptions.QueryException;
import cz.cuni.mff.odcleanstore.crbatch.ConnectionFactory;
import cz.cuni.mff.odcleanstore.crbatch.config.QueryConfig;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.URIMappingIterableImpl;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import cz.cuni.mff.odcleanstore.vocabulary.OWL;

/**
 * Loads owl:sameAs links from named graphs to be processed and their attached graphs.
 * The result is returned as an instance of {@link URIMappingIterable}.
 * @author Jan Michelfeit
 */
public class SameAsLinkLoader extends DatabaseLoaderBase {
    private static final Logger LOG = LoggerFactory.getLogger(SameAsLinkLoader.class);
    
    /**
     * SPARQL query that gets owl:sameAs links from relevant payload graphs.
     * Variable {@link #ngRestrictionVar} represents the named graph.
     * Result contains variables ?r1 ?r2 representing two resources connected by the owl:sameAs property
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     * (4) graph name prefix filter
     */
    private static final String PAYLOAD_SAMEAS_QUERY = "SPARQL %1$s"
            + "\n SELECT ?" + VAR_PREFIX + "r1 ?" + VAR_PREFIX + "r2"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   GRAPH ?%3$s {"
            + "\n     ?" + VAR_PREFIX + "r1 <" + OWL.sameAs + "> ?" + VAR_PREFIX + "r2"
            + "\n   }"
            + "\n   ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n   %4$s"
            + "\n }";
    
    /**
     * SPARQL query that gets owl:sameAs links from relevant attached graphs.
     * Variable {@link #ngRestrictionVar} represents the named graph.
     * Result contains variables ?r1 ?r2 representing two resources connected by the owl:sameAs property
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction patter
     * (3) named graph restriction variable
     * (4) graph name prefix filter
     */
    private static final String ATTACHED_SAMEAS_QUERY = "SPARQL %1$s"
            + "\n SELECT ?" + VAR_PREFIX + "r1 ?" + VAR_PREFIX + "r2"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   ?%3$s <" + ODCS.attachedGraph + "> ?" + VAR_PREFIX + "attachedGraph."
            + "\n   ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n   GRAPH ?" + VAR_PREFIX + "attachedGraph {"
            + "\n     ?" + VAR_PREFIX + "r1 <" + OWL.sameAs + "> ?" + VAR_PREFIX + "r2"
            + "\n   }"
            + "\n   %4$s"
            + "\n }";
    
    /**
     * SPARQL query that gets owl:sameAs links from ontology graphs. 
     * Contents of these graphs is not part of the output but the contained owl:sameAs links are used
     * in conflict resolution.
     * Result contains variables ?r1 ?r2 representing two resources connected by the owl:sameAs property
     *
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) ontology graph restriction patter
     * (3) ontology graph restriction variable
     * (4) ontology graph name prefix filter
     */
    private static final String ONTOLOGY_SAMEAS_QUERY = "SPARQL %1$s"
            + "\n SELECT ?" + VAR_PREFIX + "r1 ?" + VAR_PREFIX + "r2"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   GRAPH ?%3$s {"
            + "\n     ?" + VAR_PREFIX + "r1 <" + OWL.sameAs + "> ?" + VAR_PREFIX + "r2"
            + "\n   }"
            + "\n   %4$s"
            + "\n }";

    /**
     * Creates a new instance.
     * @param connectionFactory factory for database connection
     * @param queryConfig Settings for SPARQL queries  
     */
    public SameAsLinkLoader(ConnectionFactory connectionFactory, QueryConfig queryConfig) {
        super(connectionFactory, queryConfig);
    }
    
    /**
     * Loads owl:sameAs links from payload graphs matching the named graph constraint given
     * in the constructor and their attached graphs, create mapping to canonical URIs from them
     * and return it.
     * @return mapping to canonical URIs created from relevant owl:sameAs links
     * @throws CRBatchException error
     */
    public URIMappingIterable getSameAsMappings() throws CRBatchException {
        return getSameAsMappings(Collections.<String>emptySet());
    }

    /**
     * Loads owl:sameAs links from payload graphs matching the named graph constraint given
     * in the constructor and their attached graphs, create mapping to canonical URIs from them
     * and return it.
     * @param preferredURIs set of URIs preferred as canonical URIs; can be null
     * @return mapping to canonical URIs created from relevant owl:sameAs links
     * @throws CRBatchException error
     */
    public URIMappingIterable getSameAsMappings(Set<String> preferredURIs) throws CRBatchException {
        long startTime = System.currentTimeMillis();
        URIMappingIterableImpl uriMapping = new URIMappingIterableImpl(preferredURIs);
        long linkCount = 0;
        try {
            String payloadQuery = String.format(Locale.ROOT, PAYLOAD_SAMEAS_QUERY,
                    getPrefixDecl(),
                    queryConfig.getNamedGraphRestriction().getPattern(),
                    queryConfig.getNamedGraphRestriction().getVar(),
                    getSourceNamedGraphPrefixFilter());
            linkCount += loadSameAsLinks(uriMapping, payloadQuery);

            String attachedQuery = String.format(Locale.ROOT, ATTACHED_SAMEAS_QUERY,
                    getPrefixDecl(),
                    queryConfig.getNamedGraphRestriction().getPattern(),
                    queryConfig.getNamedGraphRestriction().getVar(),
                    getSourceNamedGraphPrefixFilter());
            linkCount += loadSameAsLinks(uriMapping, attachedQuery);

            if (queryConfig.getOntologyGraphRestriction() != null) {
                String ontologyQuery = String.format(Locale.ROOT, ONTOLOGY_SAMEAS_QUERY,
                        getPrefixDecl(),
                        queryConfig.getOntologyGraphRestriction().getPattern(),
                        queryConfig.getOntologyGraphRestriction().getVar(),
                        getGraphPrefixFilter(queryConfig.getOntologyGraphRestriction().getVar()));
                linkCount += loadSameAsLinks(uriMapping, ontologyQuery);
            }
        } catch (DatabaseException e) {
            throw new CRBatchException(CRBatchErrorCodes.QUERY_SAMEAS, "Database error", e);
        } finally {
            closeConnectionQuietly();
        }

        LOG.debug(String.format("CR-batch: loaded & resolved %,d owl:sameAs links in %d ms", 
                linkCount, System.currentTimeMillis() - startTime));
        return uriMapping;
    }
    
    private long loadSameAsLinks(URIMappingIterableImpl uriMapping, String query) throws DatabaseException {
        final int resource1Index = 1;
        final int resource2Index = 2;
        long linkCount = 0;
        long startTime = System.currentTimeMillis();
        WrappedResultSet resultSet = getConnection().executeSelect(query);
        LOG.debug("CR-batch: Query for owl:sameAs links took {} ms", System.currentTimeMillis() - startTime);
        try {
            while (resultSet.next()) {
                String uri1 = resultSet.getString(resource1Index);
                String uri2 = resultSet.getString(resource2Index);
                uriMapping.addLink(uri1, uri2);
                linkCount++;
            }
        } catch (SQLException e) {
            throw new QueryException(e);
        } finally {
            resultSet.closeQuietly();
        }
        
        return linkCount;
    }
}
