package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.openrdf.model.Statement;

import cz.cuni.mff.odcleanstore.crbatch.DataSource;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;

/**
 * Loads triples containing statements about a given URI resource (having the URI as their subject)
 * from multiple data sources.
 * @author Jan Michelfeit
 * @see QuadLoader
 */
public class FederatedQuadLoader implements Closeable {
    private final Collection<QuadLoader> quadLoaders;

    /**
     * Creates a new instance.
     * @param dataSources initialized data sources
     * @param alternativeURINavigator container of alternative owl:sameAs variants for URIs
     */
    public FederatedQuadLoader(Collection<DataSource> dataSources, AlternativeURINavigator alternativeURINavigator) {
        quadLoaders = new ArrayList<QuadLoader>();
        for (DataSource source : dataSources) {
            QuadLoader loader = new QuadLoader(source, alternativeURINavigator);
            quadLoaders.add(loader);
        }
    }

    /**
     * Returns quads having the given uri or one of its owl:sameAs alternatives as their subject.
     * @param uri searched subject URI
     * @return quads having the given uri or one of its owl:sameAs alternatives as their subject.
     * @throws CRBatchException error
     * @see #getQuadsForURI(String)
     */
    public Collection<Statement> getQuadsForURI(String uri) throws CRBatchException {
        Collection<Statement> quads = new ArrayList<Statement>();
        for (QuadLoader loader : quadLoaders) {
            loader.loadQuadsForURI(uri, quads);
        }
        return quads;
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        for (QuadLoader loader : quadLoaders) {
            try {
                loader.close();
            } catch (IOException e) {
                exception = e;
            }
        }
        if (exception != null) {
            throw exception;
        }
        quadLoaders.clear();
    }
}
