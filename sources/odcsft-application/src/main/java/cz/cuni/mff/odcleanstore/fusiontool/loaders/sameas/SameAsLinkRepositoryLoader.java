package cz.cuni.mff.odcleanstore.fusiontool.loaders.sameas;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolQueryException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.RepositoryLoaderBase;
import cz.cuni.mff.odcleanstore.fusiontool.source.ConstructSource;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads owl:sameAs links from named graphs to be processed.
 * TODO: apply limit/offset
 */
public class SameAsLinkRepositoryLoader extends RepositoryLoaderBase implements SameAsLinkLoader {
    private static final Logger LOG = LoggerFactory.getLogger(SameAsLinkRepositoryLoader.class);
    
    /** RDF data source. */
    protected final ConstructSource constructSource;
    
    /**
     * Creates a new instance.
     * @param constructSource an initialized construct source
     */
    public SameAsLinkRepositoryLoader(ConstructSource constructSource) {
        super(constructSource);
        this.constructSource = constructSource;
    }

    /**
     * Loads owl:sameAs links from relevant named graphs and adds them to the given canonical URI mapping.
     * @param uriMapping URI mapping where loaded links will be added
     * @return number of loaded owl:sameAs links
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException repository error
     */
    public long loadSameAsMappings(UriMappingImpl uriMapping) throws LDFusionToolException {
        long startTime = System.currentTimeMillis();
        long linkCount = 0;
        
        // Load links from processed data
        String constructQuery = addPrefixDecl(this.constructSource.getConstructQuery());
        try {
            linkCount += loadSameAsLinks(uriMapping, constructQuery.trim());
        } catch (OpenRDFException e) {
            throw new LDFusionToolQueryException(LDFusionToolErrorCodes.QUERY_SAMEAS, constructQuery, constructSource.getName(), e);
        }

        LOG.debug(String.format("ODCS-FusionTool: loaded & resolved %,d owl:sameAs links from source %s in %d ms",
                    linkCount, constructSource.getName(), System.currentTimeMillis() - startTime));
        return linkCount;
    }

    private long loadSameAsLinks(UriMappingImpl uriMapping, String query) throws OpenRDFException {
        long linkCount = 0;
        long startTime = System.currentTimeMillis();
        RepositoryConnection connection = constructSource.getRepository().getConnection();
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
