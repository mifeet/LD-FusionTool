package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.io.LargeCollectionFactory;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.util.ThrowingAbstractIterator;
import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Input loader which loads quads for each subject contained in the given collection of subjects.
 * Each call to {@link #nextQuads()} returns triples for one subject.
 * Only the subjects given in constructor are processed and no transitive discovery is done.
 */
public class SubjectsSetInputLoader implements InputLoader {
    private static final Logger LOG = LoggerFactory.getLogger(SubjectsSetInputLoader.class);

    protected final LargeCollectionFactory largeCollectionFactory;
    protected final Collection<DataSource> dataSources;
    protected final UriCollection initialSubjects;
    protected final SubjectsIterator subjectsIterator;
    protected final boolean outputMappedSubjectsOnly;
    protected UriCollection subjectsQueue = null;
    private ResourceQuadLoader resourceQuadLoader;
    private Set<String> resolvedCanonicalURIs;
    private URIMappingIterable uriMapping;

    /**
     * @param subjects collections of subjects to be processed;
     * {@code subjects} is closed when this class is closed
     * @param dataSources initialized repositories containing source data
     * {@code dataSources} are closed when this class is closed
     * @param largeCollectionFactory factory for large collections
     * {@code largeCollectionFactory} is closed when this class is closed
     * @param outputMappedSubjectsOnly see {@link cz.cuni.mff.odcleanstore.fusiontool.config.Config#getOutputMappedSubjectsOnly()}
     */
    public SubjectsSetInputLoader(
            UriCollection subjects,
            Collection<DataSource> dataSources,
            LargeCollectionFactory largeCollectionFactory,
            boolean outputMappedSubjectsOnly) {
        this.initialSubjects = subjects;
        this.dataSources = dataSources;
        this.largeCollectionFactory = largeCollectionFactory;
        this.subjectsIterator = new SubjectsIterator();
        this.outputMappedSubjectsOnly = outputMappedSubjectsOnly;
    }

    @Override
    public void initialize(URIMappingIterable uriMapping) throws ODCSFusionToolException {
        this.uriMapping = uriMapping;
        this.resourceQuadLoader = createResourceQuadLoader(dataSources, new AlternativeURINavigator(uriMapping));
        this.resolvedCanonicalURIs = largeCollectionFactory.createSet();
        this.subjectsQueue = createSubjectsQueue(initialSubjects);
    }

    @Override
    public Collection<Statement> nextQuads() throws ODCSFusionToolException {
        if (subjectsQueue == null) {
            throw new IllegalStateException("Must be initialized with initialize() first");
        }
        String canonicalURI = subjectsIterator.next();
        if (canonicalURI == null) {
            LOG.warn("No more subjects to load"); // shouldn't happen, this means that someone didn't respect hasNext()
            return Collections.emptySet();
        }
        addResolvedCanonicalUri(canonicalURI);

        Collection<Statement> quads = new ArrayList<Statement>();
        resourceQuadLoader.loadQuadsForURI(canonicalURI, quads);
        LOG.info("Loaded {} quads for URI <{}>", quads.size(), canonicalURI);
        return quads;
    }

    @Override
    public boolean hasNext() throws ODCSFusionToolException {
        if (subjectsQueue == null) {
            throw new IllegalStateException("Must be initialized with initialize() first");
        }
        return subjectsIterator.hasNext();
    }

    @Override
    public void updateWithResolvedStatements(Collection<ResolvedStatement> resolvedStatements) {
        if (subjectsQueue == null) {
            throw new IllegalStateException("Must be initialized with initialize() first");
        }
        // do nothing
    }

    @Override
    public void close() throws ODCSFusionToolException {
        try {
            subjectsQueue.close();
        } catch (IOException e) {
            LOG.error("Error closing subject queue in InputLoader", e);
        }
        try {
            initialSubjects.close();
        } catch (IOException e) {
            LOG.error("Error closing initial subjects collection in InputLoader", e);
        }
        try {
            largeCollectionFactory.close();
        } catch (IOException e) {
            LOG.error("Error closing LargeCollectionFactory in InputLoader", e);
        }
        for (DataSource dataSource : dataSources) {
            try {
                dataSource.getRepository().shutDown();
            } catch (RepositoryException e) {
                LOG.error("Error when closing repository " + dataSource, e);
            }
        }
    }

    /**
     * Returns effective canonical URI mapping.
     * @return URI mapping
     */
    protected URIMappingIterable getUriMapping() {
        return uriMapping;
    }

    /**
     * Create a queue of subjects to be processed.
     * @param initialSubjects initial subjects to be processed
     * @return return queue of subjects to be processed
     * @throws ODCSFusionToolException error
     */
    protected UriCollection createSubjectsQueue(UriCollection initialSubjects) throws ODCSFusionToolException {
        return initialSubjects;
    }

    /** Indicates whether the given canonical URI has already been resolved. */
    protected boolean isResolvedCanonicalUri(String uri) {
        return resolvedCanonicalURIs.contains(uri);
    }

    /** Adds the given canonical URI to the queue of subjects to be resolved. */
    protected void addResolvedCanonicalUri(String uri) {
        resolvedCanonicalURIs.add(uri);
    }

    /**
     * Creates a quad loader retrieving quads from the given data sources (checking all of them).
     * @param dataSources initialized data sources
     * @param alternativeURINavigator container of alternative owl:sameAs variants for URIs
     * @return initialized quad loader
     */
    protected ResourceQuadLoader createResourceQuadLoader(Collection<DataSource> dataSources, AlternativeURINavigator alternativeURINavigator) {
        if (dataSources.size() == 1) {
            return new RepositoryResourceQuadLoader(dataSources.iterator().next(), alternativeURINavigator);
        } else {
            return new FederatedResourceQuadLoader(dataSources, alternativeURINavigator);
        }
    }

    /** Iterator over canonical URIs to be resolved. */
    protected class SubjectsIterator extends ThrowingAbstractIterator<String, ODCSFusionToolException> {
        @Override
        protected String computeNext() throws ODCSFusionToolException {
            while (subjectsQueue.hasNext()) {
                String nextSubject = subjectsQueue.next();
                String canonicalURI = uriMapping.getCanonicalURI(nextSubject);

                if (outputMappedSubjectsOnly && nextSubject.equals(canonicalURI)) {
                    // Skip subjects with no mapping
                    LOG.debug("Skipping not mapped subject <{}>", nextSubject);
                    continue;
                }
                if (isResolvedCanonicalUri(canonicalURI)) {
                    continue; // avoid processing a URI multiple times
                }
                return nextSubject;
            }
            return endOfData();
        }
    }
}
