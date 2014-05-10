package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolQueryException;
import cz.cuni.mff.odcleanstore.fusiontool.source.ConstructSource;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads URIs and metadata of named graphs to be processed.
 * TODO apply limit/offset
 */
public class MetadataLoader extends RepositoryLoaderBase {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataLoader.class);

    private final ConstructSource constructSource;

    /**
     * Creates a new instance.
     *
     * @param constructSource an initialized construct source
     */
    public MetadataLoader(ConstructSource constructSource) {
        super(constructSource);
        this.constructSource = constructSource;
    }

    /**
     * Loads relevant metadata and adds them to the given metadata collection.
     *
     * @param metadata named graph metadata where loaded metadata are added
     * @throws ODCSFusionToolException repository error
     */
    public void loadNamedGraphsMetadata(Model metadata) throws ODCSFusionToolException {
        long startTime = System.currentTimeMillis();
        String query = "";
        try {
            query = addPrefixDecl(constructSource.getConstructQuery());
            loadMetadataInternal(metadata, query);
        } catch (OpenRDFException e) {
            throw new ODCSFusionToolQueryException(ODCSFusionToolErrorCodes.QUERY_NG_METADATA, query, constructSource.getName(), e);
        }

        LOG.debug("ODCS-FusionTool: Metadata loaded from source {} in {} ms",
                constructSource.getName(),
                System.currentTimeMillis() - startTime);
    }

    private void loadMetadataInternal(Model metadata, String query) throws OpenRDFException {
        long startTime = System.currentTimeMillis();
        RepositoryConnection connection = constructSource.getRepository().getConnection();
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
