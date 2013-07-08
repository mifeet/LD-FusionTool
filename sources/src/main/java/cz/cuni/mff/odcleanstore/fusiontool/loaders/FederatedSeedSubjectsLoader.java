package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import cz.cuni.mff.odcleanstore.fusiontool.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;

/**
 * Seed subjects loader which provides access to seed subjects obtained from multiple data sources.
 * @see SeedSubjectsLoader
 * @author Jan Michelfeit
 */
public class FederatedSeedSubjectsLoader {
    private final Collection<DataSource> dataSources;
    
    /**
     * Iterator wrapping access to underlying iterators for each data source.
     * At all times only one underlying iterator is opened.
     */
    private final class FederatedUriCollection implements UriCollection {
        private UriCollection currentCollection;
        private Iterator<DataSource> dataSourceIt;
        private final SparqlRestriction seedResourceRestriction;
        private String next = null;

        protected FederatedUriCollection(Collection<DataSource> dataSources, SparqlRestriction seedResourceRestriction)
                throws ODCSFusionToolException {
            this.dataSourceIt = dataSources.iterator();
            this.seedResourceRestriction = seedResourceRestriction;
            this.next = getNextResult();
        }
        
        /**
         * Returns {@code true} if the collection has more elements.
         * @return {@code true} if the collection has more elements
         */
        @Override
        public boolean hasNext() {
            return next != null;
        }

        /**
         * Returns an element from the collection and moves the iterator by one.
         * @return the current element
         * @throws ODCSFusionToolException error
         */
        @Override
        public String next() throws ODCSFusionToolException {
            if (next == null) {
                throw new NoSuchElementException();
            }
            String result = next;
            next = getNextResult();
            return result;
        }
        
        private String getNextResult() throws ODCSFusionToolException {
            if (currentCollection != null && currentCollection.hasNext()) {
                return currentCollection.next();
            }
            currentCollection = getNextCollection();
            if (currentCollection == null) {
                return null; // no more data sources
            }
            // Use recursion; the loop is not infinite because we have moved by one collection 
            return getNextResult();
        }
        
        private UriCollection getNextCollection() throws ODCSFusionToolException {
            if (dataSourceIt == null) {
                throw new NoSuchElementException();
            }
            if (dataSourceIt.hasNext()) {
                DataSource source = dataSourceIt.next();
                SeedSubjectsLoader loader = new SeedSubjectsLoader(source);
                return loader.getTripleSubjectsCollection(seedResourceRestriction);
            } else {
                return null;
            }
        }
        
        @Override
        public void close() throws IOException {
            if (currentCollection != null) {
                currentCollection.close();
                currentCollection = null;
            }
            next = null;
            dataSourceIt = null;
        }
        
        /**
         * Does nothing.
         */
        @Override
        public void add(String node) {
            // do nothing
        }
    }
    
    /**
     * Creates a new instance for the given data sources.
     * @param dataSources initialized RDF data sources
     */
    public FederatedSeedSubjectsLoader(Collection<DataSource> dataSources) {
        this.dataSources = dataSources;
    }

    /**
     * Returns iterator over all subjects of relevant triples in all data sources.
     * Returned values may not be distinct.
     * @param seedResourceRestriction SPARQL restriction on URI resources which are initially loaded and processed
     *      or null to iterate all subjects
     * @return collection of subjects of relevant triples
     * @throws ODCSFusionToolException query error
     */
    public UriCollection getTripleSubjectsCollection(SparqlRestriction seedResourceRestriction) throws ODCSFusionToolException {
        return new FederatedUriCollection(dataSources, seedResourceRestriction);
    }
}
