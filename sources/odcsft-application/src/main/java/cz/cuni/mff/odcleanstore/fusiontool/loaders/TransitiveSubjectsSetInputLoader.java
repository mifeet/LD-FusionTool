package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.URIMapping;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.LargeCollectionFactory;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.util.UriCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Input loader which loads quads for each subject contained in the given collection of subjects
 * and additionally it adds every object encountered in resolved quads to this collection.
 * Therefore all initially given subject and all resources discovered transitively are processed.
 * Each call to {@link #nextQuads()} returns triples for one subject.
 */
public class TransitiveSubjectsSetInputLoader extends SubjectsSetInputLoader {
    private static final Logger LOG = LoggerFactory.getLogger(TransitiveSubjectsSetInputLoader.class);

    /**
     * @param initialSubjects collections of subjects to be processed;
     * {@code subjects} is closed when this class is closed
     * @param dataSources initialized repositories containing source data
     * {@code dataSources} are closed when this class is closed
     * @param largeCollectionFactory factory for large collections
     * {@code largeCollectionFactory} is closed when this class is closed
     * @param outputMappedSubjectsOnly see {@link cz.cuni.mff.odcleanstore.fusiontool.config.Config#getOutputMappedSubjectsOnly()}
     */
    public TransitiveSubjectsSetInputLoader(
            UriCollection initialSubjects,
            Collection<DataSource> dataSources,
            LargeCollectionFactory largeCollectionFactory,
            boolean outputMappedSubjectsOnly) {
        super(initialSubjects, dataSources, largeCollectionFactory, outputMappedSubjectsOnly);
    }

    @Override
    protected UriCollection createSubjectsQueue(UriCollection initialSubjects) throws ODCSFusionToolException {
        UriCollection subjectsQueue = createBufferedSubjectsCollection(initialSubjects);
        try {
            // do not keep this open longer than necessary
            initialSubjects.close();
        } catch (IOException e) {
            LOG.warn("Error closing initial subject queue", e);
        }
        return subjectsQueue;
    }

    @Override
    public void updateWithResolvedStatements(Collection<ResolvedStatement> resolvedStatements) {
        super.updateWithResolvedStatements(resolvedStatements);

        // Add discovered objects to the queue
        URIMapping uriMapping = getUriMapping();
        for (ResolvedStatement resolvedStatement : resolvedStatements) {
            String uri = ODCSUtils.getNodeUri(resolvedStatement.getStatement().getObject());
            if (uri == null) {
                // not a referenceable node
                continue;
            }
            // only add canonical URIs in order to save space
            String canonicalURI = uriMapping.getCanonicalURI(uri);
            if (!isResolvedCanonicalUri(canonicalURI)) {
                // only add new URIs
                subjectsQueue.add(canonicalURI);
            }
        }
    }

    /**
     * Creates a collection to hold subject URIs queued to be processed.
     * @param seedSubjects initial URIs to fill in the collection
     * @return collection of URIs
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException error
     */
    private UriCollection createBufferedSubjectsCollection(UriCollection seedSubjects) throws ODCSFusionToolException {
        Set<String> buffer = largeCollectionFactory.createSet();
        UriCollection queuedSubjects = new BufferedSubjectsCollection(buffer);
        URIMappingIterable uriMapping = getUriMapping();
        long count = 0;
        while (seedSubjects.hasNext()) {
            String canonicalURI = uriMapping.getCanonicalURI(seedSubjects.next());
            queuedSubjects.add(canonicalURI); // only store canonical URIs to save space
            count++;
        }
        LOG.info(String.format("Buffered approx. %,d seed resources", count));
        return queuedSubjects;
    }
}
