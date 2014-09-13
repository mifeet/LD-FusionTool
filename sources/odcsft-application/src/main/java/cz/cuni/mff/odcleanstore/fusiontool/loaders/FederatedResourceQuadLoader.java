package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSource;
import org.openrdf.model.Statement;

import java.util.ArrayList;
import java.util.Collection;

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
     * @param alternativeUriNavigator container of alternative owl:sameAs variants for URIs
     */
    public FederatedResourceQuadLoader(Collection<DataSource> dataSources, AlternativeUriNavigator alternativeUriNavigator) {
        resourceQuadLoaders = new ArrayList<ResourceQuadLoader>();
        for (DataSource source : dataSources) {
            ResourceQuadLoader loader = new RepositoryResourceQuadLoader(source, alternativeUriNavigator);
            resourceQuadLoaders.add(loader);
        }
    }

    /**
     * Returns quads having the given uri or one of its owl:sameAs alternatives as their subject.
     * @param uri searched subject URI
     * @return quads having the given uri or one of its owl:sameAs alternatives as their subject.
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException error
     * @see #getQuadsForURI(String)
     */
    public Collection<Statement> getQuadsForURI(String uri) throws LDFusionToolException {
        Collection<Statement> quads = new ArrayList<Statement>();
        loadQuadsForURI(uri, quads);
        return quads;
    }
    

    @Override
    public void loadQuadsForURI(String uri, Collection<Statement> quadCollection) throws LDFusionToolException {
        for (ResourceQuadLoader loader : resourceQuadLoaders) {
            loader.loadQuadsForURI(uri, quadCollection);
        }
    }

    @Override
    public void close() throws LDFusionToolException {
        LDFusionToolException exception = null;
        for (ResourceQuadLoader loader : resourceQuadLoaders) {
            try {
                loader.close();
            } catch (LDFusionToolException e) {
                exception = e;
            }
        }
        resourceQuadLoaders.clear();
        if (exception != null) {
            throw exception;
        }
    }
}
