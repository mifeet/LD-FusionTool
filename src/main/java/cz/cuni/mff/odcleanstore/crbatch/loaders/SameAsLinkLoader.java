package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.sql.SQLException;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.URIMapping;
import cz.cuni.mff.odcleanstore.connection.WrappedResultSet;
import cz.cuni.mff.odcleanstore.connection.exceptions.DatabaseException;
import cz.cuni.mff.odcleanstore.connection.exceptions.QueryException;
import cz.cuni.mff.odcleanstore.crbatch.ConfigConstants;
import cz.cuni.mff.odcleanstore.crbatch.ConnectionFactory;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.URIMappingIterableImpl;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import cz.cuni.mff.odcleanstore.vocabulary.OWL;

/**
 * Loads owl:sameAs links from named graphs to be processed and their attached graphs.
 * The result is returned as an instance of {@link URIMapping}.
 * @author Jan Michelfeit
 */
public class SameAsLinkLoader extends DatabaseLoaderBase {
    private static final Logger LOG = LoggerFactory.getLogger(SameAsLinkLoader.class);
    
    /**
     * SPARQL query that gets owl:sameAs links from relevant payload graphs.
     * Variable {@link ConfigConstants#NG_CONSTRAINT_VAR} represents the named graph.
     * Result contains variables ?r1 ?r2 representing two resources connected by the owl:sameAs property
     * 
     * Must be formatted with arguments:
     * (1) named graph constraint pattern
     * (2) graph name prefix filter
     */
    private static final String PAYLOAD_SAMEAS_QUERY = "SPARQL"
            + "\n SELECT ?" + VAR_PREFIX + "r1 ?" + VAR_PREFIX + "r2"
            + "\n WHERE {"
            + "\n   %1$s"
            + "\n   GRAPH ?" + ConfigConstants.NG_CONSTRAINT_VAR + " {"
            + "\n     ?" + VAR_PREFIX + "r1 <" + OWL.sameAs + "> ?" + VAR_PREFIX + "r2"
            + "\n   }"
            + "\n   ?" + ConfigConstants.NG_CONSTRAINT_VAR + " <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n   %2$s"
            + "\n }";
    
    /**
     * SPARQL query that gets owl:sameAs links from relevant attached graphs.
     * Variable {@link ConfigConstants#NG_CONSTRAINT_VAR} represents the named graph.
     * Result contains variables ?r1 ?r2 representing two resources connected by the owl:sameAs property
     * 
     * Must be formatted with arguments:
     * (1) named graph constraint pattern
     * (2) graph name prefix filter
     */
    private static final String ATTACHED_SAMEAS_QUERY = "SPARQL"
            + "\n SELECT ?" + VAR_PREFIX + "r1 ?" + VAR_PREFIX + "r2"
            + "\n WHERE {"
            + "\n   %1$s"
            + "\n   ?" + ConfigConstants.NG_CONSTRAINT_VAR + " <" + ODCS.attachedGraph + "> ?" + VAR_PREFIX + "attachedGraph."
            + "\n   ?" + ConfigConstants.NG_CONSTRAINT_VAR + " <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n   GRAPH ?" + VAR_PREFIX + "attachedGraph {"
            + "\n     ?" + VAR_PREFIX + "r1 <" + OWL.sameAs + "> ?" + VAR_PREFIX + "r2"
            + "\n   }"
            + "\n   %2$s"
            + "\n }";

    private final String namedGraphConstraintPattern;

    /**
     * Creates a new instance.
     * @param connectionFactory factory for database connection
     * @param namedGraphConstraintPattern SPARQL group graph pattern limiting source payload named graphs 
     *      (where ?{@value ConfigConstants#NG_CONSTRAINT_VAR} represents the payload graph)
     */
    public SameAsLinkLoader(ConnectionFactory connFactory, String namedGraphConstraintPattern) {
        super(connFactory);
        this.namedGraphConstraintPattern = namedGraphConstraintPattern;
    }

    /**
     * @TODO javadoc
     * @return
     */
    public URIMappingIterable getSameAsMappings() throws CRBatchException {
        long startTime = System.currentTimeMillis();
        URIMappingIterableImpl uriMapping = new URIMappingIterableImpl();
        long linkCount = 0;
        try {
            String payloadQuery = String.format(Locale.ROOT, PAYLOAD_SAMEAS_QUERY,
                    namedGraphConstraintPattern, LoaderUtils.getGraphPrefixFilter(ConfigConstants.NG_CONSTRAINT_VAR));
            linkCount += loadSameAsLinks(uriMapping, payloadQuery);
            
            String attachedQuery = String.format(Locale.ROOT, ATTACHED_SAMEAS_QUERY,
                    namedGraphConstraintPattern, LoaderUtils.getGraphPrefixFilter(ConfigConstants.NG_CONSTRAINT_VAR));
            linkCount += loadSameAsLinks(uriMapping, attachedQuery);
        } catch (DatabaseException e) {
            throw new CRBatchException(CRBatchErrorCodes.QUERY_SAMEAS, "Database error", e);
        } finally {
            closeConnectionQuietly();
        }
        
        LOG.debug("CR-batch: loaded & resolved {} owl:sameAs links in {} ms", linkCount, System.currentTimeMillis() - startTime);
        return uriMapping;
    }
    
    private long loadSameAsLinks(URIMappingIterableImpl uriMapping, String query) throws DatabaseException {
        final int resource1Index = 1;
        final int resource2Index = 2;
        long linkCount = 0;
        long startTime = System.currentTimeMillis();
        WrappedResultSet resultSet = getConnection().executeSelect(query);
        LOG.debug("CR-batch: Payload owl:sameAs query took {} ms", System.currentTimeMillis() - startTime);
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
