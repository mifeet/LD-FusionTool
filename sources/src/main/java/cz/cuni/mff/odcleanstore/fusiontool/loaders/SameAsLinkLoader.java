package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import java.util.Locale;

import org.openrdf.OpenRDFException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.URIMappingImpl;
import cz.cuni.mff.odcleanstore.fusiontool.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolQueryException;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
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
    private static final String SAMEAS_QUERY = "%1$s"
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
     * @throws ODCSFusionToolException repository error
     */
    public void loadSameAsMappings(URIMappingImpl uriMapping) throws ODCSFusionToolException {
        long startTime = System.currentTimeMillis();
        long linkCount = 0;
        
        // Load links from processed data
        SparqlRestriction namedGraphRestriction;
        if (dataSource.getNamedGraphRestriction() != null) {
            namedGraphRestriction = dataSource.getNamedGraphRestriction();
        } else {
            namedGraphRestriction = EMPTY_RESTRICTION;
        }
        String dataQuery = String.format(Locale.ROOT, SAMEAS_QUERY,
                getPrefixDecl(),
                namedGraphRestriction.getPattern(),
                namedGraphRestriction.getVar());
        try {
            linkCount += loadSameAsLinks(uriMapping, dataQuery);
        } catch (OpenRDFException e) {
            throw new ODCSFusionToolQueryException(ODCSFusionToolErrorCodes.QUERY_SAMEAS, dataQuery, dataSource.getName(), e);
        }

        // Load links from metadata graphs;
        // if namedGraphRestriction was empty than this is not necessary because all links were loaded above 
        if (dataSource.getMetadataGraphRestriction() != null && !ODCSFusionToolUtils.isRestrictionEmpty(namedGraphRestriction)) { 
            String metadataQuery = String.format(Locale.ROOT, SAMEAS_QUERY,
                    getPrefixDecl(),
                    dataSource.getMetadataGraphRestriction().getPattern(),
                    dataSource.getMetadataGraphRestriction().getVar());
            try {
                linkCount += loadSameAsLinks(uriMapping, metadataQuery);
            } catch (OpenRDFException e) {
                throw new ODCSFusionToolQueryException(ODCSFusionToolErrorCodes.QUERY_SAMEAS, metadataQuery, dataSource.getName(), e);
            }
        }

        LOG.debug(String.format("ODCS-FusionTool: loaded & resolved %,d owl:sameAs links from source %s in %d ms",
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
            LOG.debug("ODCS-FusionTool: Query for owl:sameAs links took {} ms", System.currentTimeMillis() - startTime);
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
