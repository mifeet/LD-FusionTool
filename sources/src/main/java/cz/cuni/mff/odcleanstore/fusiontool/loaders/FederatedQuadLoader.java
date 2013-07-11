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
 * @see RepositoryQuadLoader
 */
public class FederatedQuadLoader implements QuadLoader {
    private final Collection<QuadLoader> quadLoaders;

    /**
     * Creates a new instance.
     * @param dataSources initialized data sources
     * @param alternativeURINavigator container of alternative owl:sameAs variants for URIs
     */
    public FederatedQuadLoader(Collection<DataSource> dataSources, AlternativeURINavigator alternativeURINavigator) {
        quadLoaders = new ArrayList<QuadLoader>();
        for (DataSource source : dataSources) {
            QuadLoader loader = new RepositoryQuadLoader(source, alternativeURINavigator);
            quadLoaders.add(loader);
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
        for (QuadLoader loader : quadLoaders) {
            loader.loadQuadsForURI(uri, quadCollection);
        }
    }

    @Override
    public void close() throws ODCSFusionToolException {
        ODCSFusionToolException exception = null;
        for (QuadLoader loader : quadLoaders) {
            try {
                loader.close();
            } catch (ODCSFusionToolException e) {
                exception = e;
            }
        }
        quadLoaders.clear();
        if (exception != null) {
            throw exception;
        }
    }
}
