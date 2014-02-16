package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import org.openrdf.OpenRDFException;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFParserRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.URIMappingImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolQueryException;
import cz.cuni.mff.odcleanstore.fusiontool.io.ConstructSource;

/**
 * Loads owl:sameAs links from named graphs to be processed.
 * @author Jan Michelfeit
 */
public class SameAsLinkLoader {
    private static final Logger LOG = LoggerFactory.getLogger(SameAsLinkLoader.class);
    
    /** RDF data source. */
    protected final ConstructSource source;
    
    /**
     * Creates a new instance.
     * @param source an initialized construct source
     */
    public SameAsLinkLoader(ConstructSource source) {
        this.source = source;
    }

    /**
     * Loads owl:sameAs links from relevant named graphs and adds them to the given canonical URI mapping.
     * @param uriMapping URI mapping where loaded links will be added
     * @return number of loaded owl:sameAs links
     * @throws ODCSFusionToolException repository error
     */
    public long loadSameAsMappings(URIMappingImpl uriMapping) throws ODCSFusionToolException {
        long startTime = System.currentTimeMillis();
        long linkCount = 0;
        
        // Load links from processed data
        // TODO: getPrefixDecl
        String constructQuery = this.source.getConstructQuery();
        try {
            linkCount += loadSameAsLinks(uriMapping, constructQuery.trim());
        } catch (OpenRDFException e) {
            throw new ODCSFusionToolQueryException(ODCSFusionToolErrorCodes.QUERY_SAMEAS, constructQuery, source.getName(), e);
        }

        LOG.debug(String.format("ODCS-FusionTool: loaded & resolved %,d owl:sameAs links from source %s in %d ms",
                    linkCount, source.getName(), System.currentTimeMillis() - startTime));
        return linkCount;
    }

    private long loadSameAsLinks(URIMappingImpl uriMapping, String query) throws OpenRDFException {
        long linkCount = 0;
        long startTime = System.currentTimeMillis();
        RepositoryConnection connection = source.getRepository().getConnection();
        GraphQueryResult resultSet = null;
        try {
            resultSet = connection.prepareGraphQuery(QueryLanguage.SPARQL, query).evaluate();
            LOG.debug("ODCS-FusionTool: Query for owl:sameAs links took {} ms", System.currentTimeMillis() - startTime);
            while (resultSet.hasNext()) {
                Statement statement = resultSet.next();
                uriMapping.addLink(statement);
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
