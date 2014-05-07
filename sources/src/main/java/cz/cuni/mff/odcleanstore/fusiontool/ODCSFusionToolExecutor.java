/**
 *
 */
package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory.ConflictResolverBuilder;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunctionRegistry;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.DistanceMeasureImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.SourceQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.impl.ODCSSourceQualityCalculator;
import cz.cuni.mff.odcleanstore.fusiontool.config.*;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.*;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.*;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.util.*;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;
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

import java.io.*;
import java.util.*;

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
        isTransitive = config.getSeedResourceRestriction() != null
            && config.getSeedResourceRestriction().isTransitive();
    }

    /**
     * Performs the actual ODCS-FusionTool task according to the given configuration.
     * @throws ODCSFusionToolException general fusion error
     * @throws IOException I/O error when writing results
     * @throws ConflictResolutionException conflict resolution error
     */
    public void runFusionTool() throws ODCSFusionToolException, IOException, ConflictResolutionException {
        InputLoader inputLoader = null;
        List<CloseableRDFWriter> rdfWriters = null;
        resetProfilers();
        try {
            timeProfiler.startCounter(EnumProfilingCounters.INITIALIZATION);
            // Load source named graphs metadata
            Model metadata = getMetadata(getConstructSources(config.getMetadataSources()));

            // Load & resolve owl:sameAs links
            Collection<ConstructSource> sameAsSources = getConstructSources(config.getSameAsSources());
            URIMappingIterable uriMapping = getURIMapping(sameAsSources);

            // Create & initialize quad loader
            inputLoader = getInputLoader();
            inputLoader.initialize(uriMapping);

            // Initialize CR
            ConflictResolver conflictResolver = createConflictResolver(metadata, uriMapping);

            // Initialize output writer
            rdfWriters = createRDFWriters();

            // Initialize triple counter
            boolean checkMaxOutputTriples = config.getMaxOutputTriples() != null && config.getMaxOutputTriples() >= 0;
            long maxOutputTriples = checkMaxOutputTriples ? config.getMaxOutputTriples() : -1;
            long outputTriples = 0;
            long inputTriples = 0;
            timeProfiler.stopAddCounter(EnumProfilingCounters.INITIALIZATION);

            // Load & process input quads
            timeProfiler.startCounter(EnumProfilingCounters.BUFFERING);
            while (inputLoader.hasNext()) {
                timeProfiler.stopAddCounter(EnumProfilingCounters.BUFFERING);

                // Load quads for the given subject
                timeProfiler.startCounter(EnumProfilingCounters.QUAD_LOADING);
                Collection<Statement> quads = inputLoader.nextQuads();
                inputTriples += quads.size();
                timeProfiler.stopAddCounter(EnumProfilingCounters.QUAD_LOADING);

                // Resolve conflicts
                timeProfiler.startCounter(EnumProfilingCounters.CONFLICT_RESOLUTION);
                Collection<ResolvedStatement> resolvedQuads = conflictResolver.resolveConflicts(quads);
                timeProfiler.stopAddCounter(EnumProfilingCounters.CONFLICT_RESOLUTION);
                LOG.info("Resolved {} quads resulting in {} quads (processed totally {} quads)",
                        new Object[] {quads.size(), resolvedQuads.size(), inputTriples});

                // Check if we have reached the limit on output triples
                if (checkMaxOutputTriples && outputTriples + resolvedQuads.size() > maxOutputTriples) {
                    break;
                }
                outputTriples += resolvedQuads.size();

                // Add objects filtered by CR for traversal
                timeProfiler.startCounter(EnumProfilingCounters.BUFFERING);
                inputLoader.updateWithResolvedStatements(resolvedQuads);
                timeProfiler.stopAddCounter(EnumProfilingCounters.BUFFERING);

                // Write result to output
                timeProfiler.startCounter(EnumProfilingCounters.OUTPUT_WRITING);
                writeOutput(rdfWriters, resolvedQuads);
                timeProfiler.stopAddCounter(EnumProfilingCounters.OUTPUT_WRITING);

                memoryProfiler.capture();
                fixVirtuosoOpenedStatements();
                timeProfiler.startCounter(EnumProfilingCounters.BUFFERING);
            }
            timeProfiler.stopAddCounter(EnumProfilingCounters.BUFFERING);
            LOG.info(String.format("Processed %,d quads which were resolved to %,d output quads.", inputTriples, outputTriples));

            writeCanonicalURIs(uriMapping, config.getCanonicalURIsOutputFile());
            writeSameAsLinks(uriMapping, config.getOutputs(), config.getPrefixes(), ValueFactoryImpl.getInstance());
            printProfilingInformation(timeProfiler, memoryProfiler);
        } finally {
            if (rdfWriters != null) {
                for (CloseableRDFWriter writer : rdfWriters) {
                    writer.close();
                }
            }
            if (inputLoader != null) {
                inputLoader.close();
            }
        }
    }

    private InputLoader getInputLoader() throws IOException, ODCSFusionToolException {
        Collection<DataSource> dataSources = getDataSources();
        UriCollection seedSubjects = getSeedSubjects(dataSources, config.getSeedResourceRestriction());
        LargeCollectionFactory largeCollectionFactory = createLargeCollectionFactory();
        if (isTransitive) {
            return new TransitiveSubjectsSetInputLoader(seedSubjects, dataSources, largeCollectionFactory, config.getOutputMappedSubjectsOnly());
        } else {
            return new SubjectsSetInputLoader(seedSubjects, dataSources, largeCollectionFactory, config.getOutputMappedSubjectsOnly());
        }
    }

    /**
     * Initializes data sources from configuration.
     * @return initialized data sources
     * @throws ODCSFusionToolException I/O error
     */
    protected Collection<DataSource> getDataSources() throws ODCSFusionToolException {
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
    protected URIMappingIterable getURIMapping(Collection<ConstructSource> sameAsSources) throws ODCSFusionToolException, IOException {
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
     * Creates factory object for large collections depending on configuration.
     * If cache is enabled, the collection is backed by a file, otherwise kept in memory.
     * @return factory for large collections
     * @throws IOException I/O error
     */
    protected LargeCollectionFactory createLargeCollectionFactory() throws IOException {
        if (config.getEnableFileCache()) {
            return new MapdbCollectionFactory(config.getWorkingDirectory());
        } else {
            return new MemoryCollectionFactory();
        }
    }

    /**
     * Creates and initializes output writers.
     * @return output writers
     * @throws IOException I/O error
     * @throws ODCSFusionToolException configuration error
     */
    protected List<CloseableRDFWriter> createRDFWriters() throws IOException, ODCSFusionToolException {
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
                config.getAgreeCoefficient(),
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
     * Write resolved quads to output.
     * @param rdfWriters list of data output writers
     * @param resolvedQuads quads to be written
     * @throws IOException I/O error
     */
    protected void writeOutput(List<CloseableRDFWriter> rdfWriters, Collection<ResolvedStatement> resolvedQuads) throws IOException {
        for (CloseableRDFWriter writer : rdfWriters) {
            writer.writeResolvedStatements(resolvedQuads.iterator());
        }
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
     * @param uriMapping canonical URI mappings
     * @param outputFile file where to write
     * @throws IOException writing error
     */
    protected void writeCanonicalURIs(URIMappingIterable uriMapping, File outputFile) throws IOException {
        if (outputFile == null) {
            return;
        }
        if (!outputFile.exists() || outputFile.canWrite()) {
            Set<String> canonicalUris = new HashSet<String>();
            for (String mappedUri : uriMapping) {
                canonicalUris.add(uriMapping.getCanonicalURI(mappedUri));
            }

            ODCSFusionToolUtils.ensureParentsExists(outputFile);
            CountingOutputStream outputStream = new CountingOutputStream(new FileOutputStream(outputFile));
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            try {
                for (String uri : canonicalUris) {
                    writer.println(uri);
                }
            } finally {
                writer.close();
            }
            LOG.info(String.format("Written %,d canonical URIs (total size %s)",
                    canonicalUris.size(),
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
