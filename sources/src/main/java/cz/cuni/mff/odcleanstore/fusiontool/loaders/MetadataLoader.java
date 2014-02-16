package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import org.openrdf.OpenRDFException;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolQueryException;
import cz.cuni.mff.odcleanstore.fusiontool.io.ConstructSource;

/**
 * Loads URIs and metadata of named graphs to be processed.
 * @author Jan Michelfeit
 */
public class MetadataLoader {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataLoader.class);
    
    private ConstructSource source;

    /**
     * Creates a new instance.
     * @param source an initialized construct source
     */
    public MetadataLoader(ConstructSource source) {
        this.source = source;
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
            // TODO: getPrefixDecl
            query = source.getConstructQuery();
            loadMetadataInternal(metadata, query);
        } catch (OpenRDFException e) {
            throw new ODCSFusionToolQueryException(ODCSFusionToolErrorCodes.QUERY_NG_METADATA, query, source.getName(), e);
        }
        
        LOG.debug("ODCS-FusionTool: Metadata loaded from source {} in {} ms", 
                source.getName(), System.currentTimeMillis() - startTime);
    }
    
    private void loadMetadataInternal(Model metadata, String query) throws OpenRDFException {
        long startTime = System.currentTimeMillis();
        RepositoryConnection connection = source.getRepository().getConnection();
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
