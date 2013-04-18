package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.util.Locale;

import org.openrdf.OpenRDFException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.URIMappingImpl;
import cz.cuni.mff.odcleanstore.crbatch.DataSource;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchQueryException;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import cz.cuni.mff.odcleanstore.vocabulary.OWL;

/**
 * Loads owl:sameAs links from named graphs to be processed.
 * @author Jan Michelfeit
 */
public class SameAsLinkLoader extends RepositoryLoaderBase {
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
     */
    private static final String PAYLOAD_SAMEAS_QUERY = "%1$s"
            + "\n SELECT ?" + VAR_PREFIX + "r1 ?" + VAR_PREFIX + "r2"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   GRAPH ?%3$s {"
            + "\n     ?" + VAR_PREFIX + "r1 <" + OWL.sameAs + "> ?" + VAR_PREFIX + "r2"
            + "\n   }"
            + "\n   ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
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
     */
    private static final String ATTACHED_SAMEAS_QUERY = "%1$s"
            + "\n SELECT ?" + VAR_PREFIX + "r1 ?" + VAR_PREFIX + "r2"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   ?%3$s <" + ODCS.attachedGraph + "> ?" + VAR_PREFIX + "attachedGraph."
            + "\n   ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n   GRAPH ?" + VAR_PREFIX + "attachedGraph {"
            + "\n     ?" + VAR_PREFIX + "r1 <" + OWL.sameAs + "> ?" + VAR_PREFIX + "r2"
            + "\n   }"
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
     */
    private static final String ONTOLOGY_SAMEAS_QUERY = "%1$s"
            + "\n SELECT ?" + VAR_PREFIX + "r1 ?" + VAR_PREFIX + "r2"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   GRAPH ?%3$s {"
            + "\n     ?" + VAR_PREFIX + "r1 <" + OWL.sameAs + "> ?" + VAR_PREFIX + "r2"
            + "\n   }"
            + "\n }";

    /**
     * Creates a new instance.
     * @param dataSource an initialized data source
     */
    public SameAsLinkLoader(DataSource dataSource) {
        super(dataSource);
    }

    /**
     * Loads owl:sameAs links from relevant named graphs and adds them to the given canonical URI mapping.
     * @param uriMapping URI mapping where loaded links will be added
     * @throws CRBatchException repository error
     */
    public void loadSameAsMappings(URIMappingImpl uriMapping) throws CRBatchException {
        long startTime = System.currentTimeMillis();
        long linkCount = 0;
        String payloadQuery = String.format(Locale.ROOT, PAYLOAD_SAMEAS_QUERY,
                getPrefixDecl(),
                dataSource.getNamedGraphRestriction().getPattern(),
                dataSource.getNamedGraphRestriction().getVar());
        try {
            linkCount += loadSameAsLinks(uriMapping, payloadQuery);
        } catch (OpenRDFException e) {
            throw new CRBatchQueryException(CRBatchErrorCodes.QUERY_SAMEAS, payloadQuery, dataSource.getName(), e);
        }

        String attachedQuery = String.format(Locale.ROOT, ATTACHED_SAMEAS_QUERY,
                getPrefixDecl(),
                dataSource.getNamedGraphRestriction().getPattern(),
                dataSource.getNamedGraphRestriction().getVar());
        try {
            linkCount += loadSameAsLinks(uriMapping, attachedQuery);
        } catch (OpenRDFException e) {
            throw new CRBatchQueryException(CRBatchErrorCodes.QUERY_SAMEAS, attachedQuery, dataSource.getName(), e);
        }

        if (dataSource.getMetadataGraphRestriction() != null) {
            String ontologyQuery = String.format(Locale.ROOT, ONTOLOGY_SAMEAS_QUERY,
                    getPrefixDecl(),
                    dataSource.getMetadataGraphRestriction().getPattern(),
                    dataSource.getMetadataGraphRestriction().getVar());
            try {
                linkCount += loadSameAsLinks(uriMapping, ontologyQuery);
            } catch (OpenRDFException e) {
                throw new CRBatchQueryException(CRBatchErrorCodes.QUERY_SAMEAS, ontologyQuery, dataSource.getName(), e);
            }
        }

        LOG.debug(String.format("CR-batch: loaded & resolved %,d owl:sameAs links from source %s in %d ms",
                    linkCount, dataSource.getName(), System.currentTimeMillis() - startTime));
    }

    private long loadSameAsLinks(URIMappingImpl uriMapping, String query) throws OpenRDFException {
        final String var1 = VAR_PREFIX + "r1";
        final String var2 = VAR_PREFIX + "r2";

        long linkCount = 0;
        long startTime = System.currentTimeMillis();
        RepositoryConnection connection = dataSource.getRepository().getConnection();
        TupleQueryResult resultSet = null;
        try {
            resultSet = connection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            LOG.debug("CR-batch: Query for owl:sameAs links took {} ms", System.currentTimeMillis() - startTime);
            while (resultSet.hasNext()) {
                BindingSet bindings = resultSet.next();
                String uri1 = bindings.getValue(var1).stringValue();
                String uri2 = bindings.getValue(var2).stringValue();
                uriMapping.addLink(uri1, uri2);
                linkCount++;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            connection.close();
        }

        return linkCount;
    }
}
