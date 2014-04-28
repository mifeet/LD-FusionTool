/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cz.cuni.mff.odcleanstore.fusiontool.loaders.FederatedResourceQuadLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.ResourceQuadLoader;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory.ConflictResolverBuilder;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunctionRegistry;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.URIMapping;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.DistanceMeasureImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.SourceQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.impl.ODCSSourceQualityCalculator;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.Config;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConstructSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumOutputType;
import cz.cuni.mff.odcleanstore.fusiontool.config.Output;
import cz.cuni.mff.odcleanstore.fusiontool.config.OutputImpl;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.io.CloseableRDFWriterFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.ConstructSource;
import cz.cuni.mff.odcleanstore.fusiontool.io.ConstructSourceImpl;
import cz.cuni.mff.odcleanstore.fusiontool.io.CountingOutputStream;
import cz.cuni.mff.odcleanstore.fusiontool.io.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.io.DataSourceImpl;
import cz.cuni.mff.odcleanstore.fusiontool.io.LargeCollectionFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.MapdbCollectionFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.MemoryCollectionFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryFactory;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.BufferedSubjectsCollection;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.FederatedSeedSubjectsLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.MetadataLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.RepositoryResourceQuadLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.SameAsLinkLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.UriCollection;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.util.ConflictingClusterConflictClusterFilter;
import cz.cuni.mff.odcleanstore.fusiontool.util.ConvertingIterator;
import cz.cuni.mff.odcleanstore.fusiontool.util.EnumProfilingCounters;
import cz.cuni.mff.odcleanstore.fusiontool.util.GenericConverter;
import cz.cuni.mff.odcleanstore.fusiontool.util.MemoryProfiler;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.ProfilingTimeCounter;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;

/**
 * Fuses RDF data loaded from RDF sources using ODCS Conflict Resolution and writes the output to RDF outputs.
 * Conflict resolution includes resolution of owl:sameAs link, resolution of instance-level conflicts.
 * See sample configuration files (sample-config-full.xml) for overview of all processing options.
 *
 * This class is not thread-safe.
 * @author Jan Michelfeit
 */
public class ODCSFusionToolExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ODCSFusionToolExecutor.class);

    /** An instance of {@link CloseableRDFWriterFactory}. */
    protected static final CloseableRDFWriterFactory RDF_WRITER_FACTORY = new CloseableRDFWriterFactory();
    
    /** An instance of {@link RepositoryFactory}. */
    protected static final RepositoryFactory REPOSITORY_FACTORY = new RepositoryFactory();

    /** Global configuration. */
    protected final Config config;

    /** Indicates if resources to process should be discovered transitively. */
    protected final boolean isTransitive;

    /** Performance profiler counter. */
    protected ProfilingTimeCounter<EnumProfilingCounters> timeProfiler;

    /** Memory profiler. */
    protected MemoryProfiler memoryProfiler;

    private final boolean hasVirtuosoSource;

    /**
     * Creates new instance.
     * @param config global configuration
     */
    public ODCSFusionToolExecutor(Config config) {
        this.config = config;
        hasVirtuosoSource = hasVirtuosoSource(config.getDataSources());
        isTransitive = config.isProcessingTransitive() && config.getSeedResourceRestriction() != null;
    }

    /**
     * Performs the actual ODCS-FusionTool task according to the given configuration.
     * @throws ODCSFusionToolException general fusion error
     * @throws IOException I/O error when writing results
     * @throws ConflictResolutionException conflict resolution error
     */
    public void runFusionTool() throws ODCSFusionToolException, IOException, ConflictResolutionException {
        LargeCollectionFactory collectionFactory = createLargeCollectionFactory();
        ResourceQuadLoader resourceQuadLoader = null;
        UriCollection queuedSubjects = null;
        List<CloseableRDFWriter> rdfWriters = null;
        Collection<DataSource> dataSources = getDataSources();
        resetProfilers();
        timeProfiler.startCounter(EnumProfilingCounters.INITIALIZATION);
        try {
            // Load source named graphs metadata
            Model metadata = getMetadata(getConstructSources(config.getMetadataSources()));

            // Load & resolve owl:sameAs links
            Collection<ConstructSource> sameAsSources = getConstructSources(config.getSameAsSources());
            URIMappingIterable uriMapping = getURIMapping(sameAsSources);
            Set<String> resolvedCanonicalURIs = collectionFactory.createSet();

            // Get iterator over subjects of relevant triples
            UriCollection seedSubjects = getSeedSubjects(dataSources, config.getSeedResourceRestriction());
            if (isTransitive) {
                queuedSubjects = createBufferedSubjectsCollection(seedSubjects, uriMapping, collectionFactory);
                seedSubjects.close();
            } else {
                queuedSubjects = seedSubjects;
            }

            // Initialize CR
            ConflictResolver conflictResolver = createConflictResolver(metadata, uriMapping);

            // Initialize output writer
            rdfWriters = createRDFWriters();

            // Initialize triple counter
            boolean checkMaxOutputTriples = config.getMaxOutputTriples() != null && config.getMaxOutputTriples() >= 0;
            long maxOutputTriples = checkMaxOutputTriples ? config.getMaxOutputTriples() : -1;
            long outputTriples = 0;
            long inputTriples = 0;

            // Load & process relevant triples (quads) subject by subject so that we can apply CR to them
            AlternativeURINavigator alternativeURINavigator = new AlternativeURINavigator(uriMapping);
            resourceQuadLoader = createResourceQuadLoader(dataSources, alternativeURINavigator);
            timeProfiler.stopAddCounter(EnumProfilingCounters.INITIALIZATION);
            while (queuedSubjects.hasNext()) {
                timeProfiler.startCounter(EnumProfilingCounters.BUFFERING);
                String uri = queuedSubjects.next();
                String canonicalURI = uriMapping.getCanonicalURI(uri);

                if (config.getOutputMappedSubjectsOnly() && uri.equals(canonicalURI)) {
                    // Skip subjects with no mapping
                    LOG.debug("Skipping not mapped subject <{}>", uri);
                    continue;
                }
                if (resolvedCanonicalURIs.contains(canonicalURI)) {
                    // avoid processing a URI multiple times
                    continue;
                }
                resolvedCanonicalURIs.add(canonicalURI);
                timeProfiler.stopAddCounter(EnumProfilingCounters.BUFFERING);

                // Load quads for the given subject
                Collection<Statement> quads = getQuads(resourceQuadLoader, canonicalURI);
                inputTriples += quads.size();

                // Resolve conflicts
                Collection<ResolvedStatement> resolvedQuads = resolveConflicts(conflictResolver, quads, canonicalURI, inputTriples);

                // Check if we have reached the limit on output triples
                if (checkMaxOutputTriples && outputTriples + resolvedQuads.size() > maxOutputTriples) {
                    break;
                }
                outputTriples += resolvedQuads.size();

                // Add objects filtered by CR for traversal
                if (isTransitive) {
                    addDiscoveredObjects(queuedSubjects, resolvedQuads, uriMapping, resolvedCanonicalURIs);
                }

                // Write result to output
                writeOutput(rdfWriters, resolvedQuads);

                memoryProfiler.capture();
                fixVirtuosoOpenedStatements();
            }
            LOG.info(String.format("Processed %,d quads which were resolved to %,d output quads.", inputTriples, outputTriples));

            writeCanonicalURIs(resolvedCanonicalURIs, config.getCanonicalURIsOutputFile());
            writeSameAsLinks(uriMapping, config.getOutputs(), config.getPrefixes(), ValueFactoryImpl.getInstance());
            printProfilingInformation(timeProfiler, memoryProfiler);
        } finally {
            if (resourceQuadLoader != null) {
                resourceQuadLoader.close();
            }
            if (queuedSubjects != null) {
                queuedSubjects.close();
            }
            if (rdfWriters != null) {
                for (CloseableRDFWriter writer : rdfWriters) {
                    writer.close();
                }
            }
            for (DataSource dataSource : dataSources) {
                try {
                    dataSource.getRepository().shutDown();
                } catch (RepositoryException e) {
                    LOG.error("Error when closing repository");
                }
            }
            if (collectionFactory != null) {
                collectionFactory.close();
            }
        }
    }

    /**
     * Do the actual conflict resolution on given quads.
     * @param conflictResolver conflict resolver instance to be used
     * @param quads quads to be resolved
     * @param canonicalURI URI currently being resolved
     * @param totalProcessedTriples number of input triples processed so far
     * @return resolved quads
     * @throws ConflictResolutionException resolution error
     */
    protected Collection<ResolvedStatement> resolveConflicts(
            ConflictResolver conflictResolver,
            Collection<Statement> quads,
            String canonicalURI,
            long totalProcessedTriples) throws ConflictResolutionException {

        timeProfiler.startCounter(EnumProfilingCounters.CONFLICT_RESOLUTION);
        Collection<ResolvedStatement> resolvedQuads = conflictResolver.resolveConflicts(quads);
        timeProfiler.stopAddCounter(EnumProfilingCounters.CONFLICT_RESOLUTION);
        LOG.info("Resolved {} quads for URI <{}> resulting in {} quads (processed totally {} quads)",
                new Object[] {quads.size(), canonicalURI, resolvedQuads.size(), totalProcessedTriples});
        return resolvedQuads;
    }

    /**
     * Creates a collection to hold subject URIs queued to be processed.
     * @param seedSubjects initial URIs to fill in the collection
     * @param uriMapping canonical URI mapping
     * @param collectionFactory factory method for creating the returned collection
     * @return collection of URIs
     * @throws ODCSFusionToolException error
     */
    protected UriCollection createBufferedSubjectsCollection(UriCollection seedSubjects, URIMapping uriMapping,
            LargeCollectionFactory collectionFactory) throws ODCSFusionToolException {
        Set<String> buffer = collectionFactory.createSet();
        UriCollection queuedSubjects = new BufferedSubjectsCollection(buffer);
        long count = 0;
        while (seedSubjects.hasNext()) {
            String canonicalURI = uriMapping.getCanonicalURI(seedSubjects.next());
            queuedSubjects.add(canonicalURI); // only store canonical URIs to save space
            count++;
        }
        LOG.info(String.format("ODCS-FusionTool: buffered approx. %,d seed resources", count));
        return queuedSubjects;
    }

    /**
     * Initializes data sources from configuration.
     * @return initialized data sources
     * @throws ODCSFusionToolException I/O error
     */
    protected Collection<DataSource> getDataSources()
            throws ODCSFusionToolException {
        List<DataSource> dataSources = new ArrayList<DataSource>();
        for (DataSourceConfig dataSourceConfig : config.getDataSources()) {
            try {
                DataSource dataSource = DataSourceImpl.fromConfig(dataSourceConfig, config.getPrefixes(), REPOSITORY_FACTORY);
                dataSources.add(dataSource);
            } catch (ODCSFusionToolException e) {
                // clean up already initialized repositories
                for (DataSource initializedDataSource : dataSources) {
                    try {
                        initializedDataSource.getRepository().shutDown();
                    } catch (RepositoryException e2) {
                        // ignore
                    }
                }
                throw e;
            }
        }
        return dataSources;
    }

    /**
     * Initializes construct sources from configuration.
     * @param constructSourceConfigs configuration for data sources
     * @return initialized construct sources
     * @throws ODCSFusionToolException I/O error
     */
    protected Collection<ConstructSource> getConstructSources(List<ConstructSourceConfig> constructSourceConfigs)
            throws ODCSFusionToolException {
        List<ConstructSource> constructSources = new ArrayList<ConstructSource>();
        for (ConstructSourceConfig constructSourceConfig : constructSourceConfigs) {
            try {
                ConstructSource constructSource =
                        ConstructSourceImpl.fromConfig(constructSourceConfig, config.getPrefixes(), REPOSITORY_FACTORY);
                constructSources.add(constructSource);
            } catch (ODCSFusionToolException e) {
                // clean up already initialized repositories
                for (ConstructSource initializedDataSource : constructSources) {
                    try {
                        initializedDataSource.getRepository().shutDown();
                    } catch (RepositoryException e2) {
                        // ignore
                    }
                }
                throw e;
            }
        }
        return constructSources;
    }

    /**
     * Returns metadata for conflict resolution.
     * @param metadataSources initialized metadata sources
     * @return metadata for conflict resolution
     * @throws ODCSFusionToolException error
     */
    protected Model getMetadata(Collection<ConstructSource> metadataSources) throws ODCSFusionToolException {
        Model metadata = new TreeModel();
        for (ConstructSource source : metadataSources) {
            MetadataLoader loader = new MetadataLoader(source);
            loader.loadNamedGraphsMetadata(metadata);
        }
        return metadata;
    }

    /**
     * Returns mapping of URIs to their canonical URI created from owl:sameAs links loaded
     * from the given data sources.
     * @param sameAsSources URI mapping sources
     * @return mapping of URIs to their canonical URI
     * @throws ODCSFusionToolException error
     * @throws IOException I/O error
     */
    protected URIMappingIterable getURIMapping(Collection<ConstructSource> sameAsSources)
            throws ODCSFusionToolException, IOException {
        Set<String> preferredURIs = getPreferredURIs(
                config.getPropertyResolutionStrategies().keySet(),
                config.getCanonicalURIsInputFile(),
                config.getPreferredCanonicalURIs());
        URIMappingIterableImpl uriMapping = new URIMappingIterableImpl(preferredURIs);
        for (ConstructSource source : sameAsSources) {
            SameAsLinkLoader loader = new SameAsLinkLoader(source);
            loader.loadSameAsMappings(uriMapping);
        }
        return uriMapping;
    }

    /**
     * Returns collections of seed subjects, i.e. the initial URIs for which
     * corresponding quads are loaded and resolved.
     * @param dataSources initialized data sources
     * @param seedResourceRestriction SPARQL restriction on URI resources which are initially loaded and processed
     *      or null to iterate all subjects
     * @return collection of seed subject URIs
     * @throws ODCSFusionToolException query error
     */
    protected UriCollection getSeedSubjects(Collection<DataSource> dataSources, SparqlRestriction seedResourceRestriction)
            throws ODCSFusionToolException {
        FederatedSeedSubjectsLoader loader = new FederatedSeedSubjectsLoader(dataSources);
        return loader.getTripleSubjectsCollection(seedResourceRestriction);
    }

    /**
     * Loads quads having the given URI as their subject using the given quad loader.
     * @param resourceQuadLoader quad loader
     * @param canonicalURI canonical URI for which quads should be loaded
     * @return collection of quads loaded by resourceQuadLoader for canonicalURI
     * @throws ODCSFusionToolException query error
     */
    protected Collection<Statement> getQuads(ResourceQuadLoader resourceQuadLoader, String canonicalURI) throws ODCSFusionToolException {
        timeProfiler.startCounter(EnumProfilingCounters.QUAD_LOADING);
        Collection<Statement> quads = new ArrayList<Statement>();
        resourceQuadLoader.loadQuadsForURI(canonicalURI, quads);
        timeProfiler.stopAddCounter(EnumProfilingCounters.QUAD_LOADING);
        return quads;
    }

    /**
     * Creates factory object for large collections depending on configuration.
     * If cache is enabled, the collection is backed by a file, otherwise kept in memory.
     * @return factory for large collections
     * @throws IOException I/O error
     */
    protected LargeCollectionFactory createLargeCollectionFactory() throws IOException {
        if (config.getEnableFileCache()) {
            return new MapdbCollectionFactory(ODCSFusionToolUtils.getCacheDirectory(config.getTempDirectory()));
        } else {
            return new MemoryCollectionFactory();
        }
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

    /**
     * Creates and initializes output writers.
     * @return output writers
     * @throws IOException I/O error
     * @throws ODCSFusionToolException configuration error
     */
    protected List<CloseableRDFWriter> createRDFWriters()
            throws IOException, ODCSFusionToolException {
        List<CloseableRDFWriter> writers = new LinkedList<CloseableRDFWriter>();
        for (Output output : config.getOutputs()) {
            CloseableRDFWriter writer = RDF_WRITER_FACTORY.createRDFWriter(output);
            writers.add(writer);
            writeNamespaceDeclarations(writer, config.getPrefixes());
        }
        return writers;
    }

    /**
     * Creates conflict resolver initialized according to given configuration.
     * @param metadata metadata for conflict resolution
     * @param uriMapping mapping of URIs to their canonical URI
     * @return initialized conflict resolver
     */
    protected ConflictResolver createConflictResolver(Model metadata, URIMappingIterable uriMapping) {

        SourceQualityCalculator sourceConfidence = new ODCSSourceQualityCalculator(
                config.getScoreIfUnknown(),
                config.getPublisherScoreWeight());
        ResolutionFunctionRegistry registry = ConflictResolverFactory.createInitializedResolutionFunctionRegistry(
                sourceConfidence,
                config.getAgreeCoeficient(),
                new DistanceMeasureImpl());

        ConflictResolverBuilder builder = ConflictResolverFactory.configureResolver()
                .setResolutionFunctionRegistry(registry)
                .setResolvedGraphsURIPrefix(config.getResultDataURIPrefix() + ODCSInternal.QUERY_RESULT_GRAPH_URI_INFIX)
                .setMetadata(metadata)
                .setURIMapping(uriMapping)
                .setDefaultResolutionStrategy(config.getDefaultResolutionStrategy())
                .setPropertyResolutionStrategies(config.getPropertyResolutionStrategies());
        if (config.getOutputConflictsOnly()) {
            builder.setConflictClusterFilter(new ConflictingClusterConflictClusterFilter());
        }

        return builder.create();
    }

    /**
     * Returns set of URIs preferred for canonical URIs.
     * The URIs are loaded from canonicalURIsInputFile if given and URIs present in settingsPreferredURIs are added.
     * @param settingsPreferredURIs URIs occurring on fusion tool configuration
     * @param canonicalURIsInputFile file with canonical URIs to be loaded; can be null
     * @param preferredCanonicalURIs default set of preferred canonical URIs
     * @return set of URIs preferred for canonical URIs
     * @throws IOException error reading canonical URIs from file
     */
    protected Set<String> getPreferredURIs(Set<URI> settingsPreferredURIs, File canonicalURIsInputFile,
            Collection<String> preferredCanonicalURIs) throws IOException {

        Set<String> preferredURIs = new HashSet<String>(settingsPreferredURIs.size());
        for (URI uri : settingsPreferredURIs) {
            preferredURIs.add(uri.stringValue());
        }
        if (canonicalURIsInputFile != null) {
            if (canonicalURIsInputFile.isFile() && canonicalURIsInputFile.canRead()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        new FileInputStream(canonicalURIsInputFile), "UTF-8"));
                String line = reader.readLine();
                while (line != null) {
                    preferredURIs.add(line);
                    line = reader.readLine();
                }
                reader.close();
            } else {
                LOG.warn("Cannot read canonical URIs from '{}'. The file may have not been created yet.",
                        canonicalURIsInputFile.getPath());
            }
        }
        preferredURIs.addAll(preferredCanonicalURIs);

        return preferredURIs;
    }

    /**
     * Adds URIs from objects of resolved statements to the given collection of queued subjects.
     * Only URIs that haven't been resolved already are added.
     * @param queuedSubjects collection where URIs are added
     * @param resolvedStatements resolved statements whose objects are added to queued subjects
     * @param uriMapping mapping to canonical URIs
     * @param resolvedCanonicalURIs set of already resolved URIs
     */
    protected void addDiscoveredObjects(UriCollection queuedSubjects,
            Collection<ResolvedStatement> resolvedStatements, URIMappingIterable uriMapping,
            Set<String> resolvedCanonicalURIs) {

        timeProfiler.startCounter(EnumProfilingCounters.BUFFERING);
        for (ResolvedStatement resolvedStatement : resolvedStatements) {
            String uri = ODCSUtils.getVirtuosoNodeURI(resolvedStatement.getStatement().getObject());
            if (uri == null) {
                // a literal or something, skip it
                continue;
            }
            // only add canonical URIs to save space
            String canonicalURI = uriMapping.getCanonicalURI(uri);
            if (!resolvedCanonicalURIs.contains(canonicalURI)) {
                // only add new URIs
                queuedSubjects.add(canonicalURI);
            }
        }
        timeProfiler.stopAddCounter(EnumProfilingCounters.BUFFERING);
    }

    /**
     * Write resolved quads to output.
     * @param rdfWriters list of data output writers
     * @param resolvedQuads quads to be written
     * @throws IOException I/O error
     */
    protected void writeOutput(List<CloseableRDFWriter> rdfWriters, Collection<ResolvedStatement> resolvedQuads) throws IOException {
        timeProfiler.startCounter(EnumProfilingCounters.OUTPUT_WRITING);
        for (CloseableRDFWriter writer : rdfWriters) {
            writer.writeResolvedStatements(resolvedQuads.iterator());
        }
        timeProfiler.stopAddCounter(EnumProfilingCounters.OUTPUT_WRITING);
    }

    /**
     * Writes namespace declarations to the given output writer.
     * @param writer output writer
     * @param nsPrefixes map of namespace prefixes
     * @throws IOException I/O error
     */
    protected void writeNamespaceDeclarations(CloseableRDFWriter writer, Map<String, String> nsPrefixes) throws IOException {
        for (Map.Entry<String, String> entry : nsPrefixes.entrySet()) {
            writer.addNamespace(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Write the given set of canonical URIs to a file, one URI per line.
     * @param resolvedCanonicalURIs URIs to write
     * @param outputFile file where to write
     * @throws IOException writing error
     */
    protected void writeCanonicalURIs(Set<String> resolvedCanonicalURIs, File outputFile) throws IOException {
        if (outputFile == null) {
            return;
        }
        if (!outputFile.exists() || outputFile.canWrite()) {
            ODCSFusionToolUtils.ensureParentsExists(outputFile);
            CountingOutputStream outputStream = new CountingOutputStream(new FileOutputStream(outputFile));
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            try {
                for (String uri : resolvedCanonicalURIs) {
                    writer.println(uri);
                }
            } finally {
                writer.close();
            }
            LOG.info(String.format("Written %,d canonical URIs (total size %s)",
                    resolvedCanonicalURIs.size(),
                    ODCSFusionToolUtils.humanReadableSize(outputStream.getByteCount())));
        } else {
            LOG.error("Cannot write canonical URIs to '{}'", outputFile.getPath());
            // Intentionally do not throw an exception
        }
    }

    /**
     * Writes owl:sameAs links according to the given URI mapping to given outputs.
     * owl:sameAs links between URIs and their canonical equivalent are written.
     * @param uriMapping uri mapping defining the links to write
     * @param outputs outputs where sameAs links are written
     * @param nsPrefixes map of namespace prefixes
     * @param valueFactory a value factory
     * @throws IOException I/O error
     * @throws ODCSFusionToolException error when creating output
     */
    protected void writeSameAsLinks(final URIMappingIterable uriMapping, List<Output> outputs,
            Map<String, String> nsPrefixes, final ValueFactory valueFactory) throws IOException, ODCSFusionToolException {

        List<CloseableRDFWriter> writers = new LinkedList<CloseableRDFWriter>();
        try {
            // Create output writers
            for (Output output : outputs) {
                if (output.getType() != EnumOutputType.FILE || output.getParams().get(Output.SAME_AS_FILE_PARAM) == null) {
                    continue;
                }

                OutputImpl sameAsOutput = new OutputImpl(EnumOutputType.FILE, output.toString() + "-sameAs");
                sameAsOutput.getParams().put(Output.PATH_PARAM, output.getParams().get(Output.SAME_AS_FILE_PARAM));
                sameAsOutput.getParams().put(Output.FORMAT_PARAM, output.getParams().get(Output.FORMAT_PARAM));
                sameAsOutput.getParams().put(Output.SPLIT_BY_MB_PARAM, output.getParams().get(Output.SPLIT_BY_MB_PARAM));
                CloseableRDFWriter writer = RDF_WRITER_FACTORY.createRDFWriter(sameAsOutput);
                writers.add(writer);
                writer.addNamespace("owl", OWL.NAMESPACE);
                writeNamespaceDeclarations(writer, nsPrefixes);
            }
            if (writers.isEmpty()) {
                return;
            }

            GenericConverter<String, Statement> uriToTripleConverter = new GenericConverter<String, Statement>() {
                @Override
                public Statement convert(String uri) {
                    String canonicalUri = uriMapping.getCanonicalURI(uri);
                    return valueFactory.createStatement(
                            valueFactory.createURI(uri),
                            org.openrdf.model.vocabulary.OWL.SAMEAS,
                            valueFactory.createURI(canonicalUri));
                }
            };

            for (CloseableRDFWriter writer : writers) {
                Iterator<String> uriIterator = uriMapping.iterator();
                Iterator<Statement> sameAsTripleIterator = ConvertingIterator.decorate(uriIterator, uriToTripleConverter);
                writer.writeQuads(sameAsTripleIterator);
            }

            LOG.info("Written owl:sameAs links");
        } finally {
            for (CloseableRDFWriter writer : writers) {
                writer.close();
            }
        }
    }

    /**
     * Indicates whether a source of type {@link EnumDataSourceType#VIRTUOSO} is present in the given data sources.
     * @param sources configuration of data sources
     * @return true iff sources contain a Virtuoso data source
     */
    protected boolean hasVirtuosoSource(List<DataSourceConfig> sources) {
        for (DataSourceConfig sourceConfig : sources) {
            if (sourceConfig.getType() == EnumDataSourceType.VIRTUOSO) {
                return true;
            }
        }
        return false;
    }

    /**
     * Fixes bug in Virtuoso which doesn't release connections even when they are released explicitly.
     * This method puts the current thread to sleep so that the thread releasing connections has chance to be
     * planned for execution. If hasVirtuosoSource is false, does nothing.
     */
    protected void fixVirtuosoOpenedStatements() {
        if (hasVirtuosoSource) {
            // Somehow helps Virtuoso release connections. Without call to Thread.sleep(),
            // application may fail with "No buffer space available (maximum connections reached?)"
            // exception for too many named graphs.
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * Initializes profiler objects {@link #timeProfiler} and {@link #memoryProfiler}.
     */
    protected void resetProfilers() {
        timeProfiler = ProfilingTimeCounter.createInstance(EnumProfilingCounters.class, config.isProfilingOn());
        memoryProfiler = MemoryProfiler.createInstance(config.isProfilingOn());
    }

    /**
     * Prints profiling information from the given profiling time counter.
     * @param timeProfiler profiling time counter
     * @param memoryProfiler memory profiler
     */
    protected void printProfilingInformation(
            ProfilingTimeCounter<EnumProfilingCounters> timeProfiler, MemoryProfiler memoryProfiler) {

        if (config.isProfilingOn()) {
            System.out.println("Initialization time:      " + timeProfiler.formatCounter(EnumProfilingCounters.INITIALIZATION));
            System.out.println("Quad loading time:        " + timeProfiler.formatCounter(EnumProfilingCounters.QUAD_LOADING));
            System.out.println("Conflict resolution time: " + timeProfiler.formatCounter(EnumProfilingCounters.CONFLICT_RESOLUTION));
            System.out.println("Buffering time:           " + timeProfiler.formatCounter(EnumProfilingCounters.BUFFERING));
            System.out.println("Output writing time:      " + timeProfiler.formatCounter(EnumProfilingCounters.OUTPUT_WRITING));
            System.out.println("Maximum total memory:     " + memoryProfiler.formatMaxTotalMemory());
        }
    }
}
