package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import java.util.ArrayList;
import java.util.Collection;

import org.openrdf.model.Statement;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.AlternativeURINavigator;

/**
 * Loads triples containing statements about a given URI resource (having the URI as their subject)
 * from multiple data sources.
 * @author Jan Michelfeit
 * @see RepositoryResourceQuadLoader
 */
public class FederatedResourceQuadLoader implements ResourceQuadLoader {
    private final Collection<ResourceQuadLoader> resourceQuadLoaders;

    /**
     * Creates a new instance.
     * @param dataSources initialized data sources
     * @param alternativeURINavigator container of alternative owl:sameAs variants for URIs
     */
    public FederatedResourceQuadLoader(Collection<DataSource> dataSources, AlternativeURINavigator alternativeURINavigator) {
        resourceQuadLoaders = new ArrayList<ResourceQuadLoader>();
        for (DataSource source : dataSources) {
            ResourceQuadLoader loader = new RepositoryResourceQuadLoader(source, alternativeURINavigator);
            resourceQuadLoaders.add(loader);
        }
    }

    /**
     * Returns quads having the given uri or one of its owl:sameAs alternatives as their subject.
     * @param uri searched subject URI
     * @return quads having the given uri or one of its owl:sameAs alternatives as their subject.
     * @throws ODCSFusionToolException error
     * @see #getQuadsForURI(String)
     */
    public Collection<Statement> getQuadsForURI(String uri) throws ODCSFusionToolException {
        Collection<Statement> quads = new ArrayList<Statement>();
        loadQuadsForURI(uri, quads);
        return quads;
    }
    

    @Override
    public void loadQuadsForURI(String uri, Collection<Statement> quadCollection) throws ODCSFusionToolException {
        for (ResourceQuadLoader loader : resourceQuadLoaders) {
            loader.loadQuadsForURI(uri, quadCollection);
        }
    }

    @Override
    public void close() throws ODCSFusionToolException {
        ODCSFusionToolException exception = null;
        for (ResourceQuadLoader loader : resourceQuadLoaders) {
            try {
                loader.close();
            } catch (ODCSFusionToolException e) {
                exception = e;
            }
        }
        resourceQuadLoaders.clear();
        if (exception != null) {
            throw exception;
        }
    }
}
