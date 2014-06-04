package cz.cuni.mff.odcleanstore.fusiontool;

import com.google.code.externalsorting.ExternalSort;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunctionRegistry;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictResolutionPolicyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.DistanceMeasureImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.SourceQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.impl.ODCSSourceQualityCalculator;
import cz.cuni.mff.odcleanstore.fusiontool.config.Config;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigConstants;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConstructSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumOutputType;
import cz.cuni.mff.odcleanstore.fusiontool.config.Output;
import cz.cuni.mff.odcleanstore.fusiontool.config.OutputImpl;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.ResourceDescriptionConflictResolverImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.CountingOutputStream;
import cz.cuni.mff.odcleanstore.fusiontool.io.LargeCollectionFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.MapdbCollectionFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.MemoryCollectionFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryFactory;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.ExternalSortingInputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.SubjectsSetInputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.TransitiveSubjectsSetInputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesFileLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesRepositoryLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.entity.FederatedSeedSubjectsLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.metadata.MetadataLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.sameas.SameAsLinkFileLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.sameas.SameAsLinkRepositoryLoader;
import cz.cuni.mff.odcleanstore.fusiontool.source.ConstructSource;
import cz.cuni.mff.odcleanstore.fusiontool.source.ConstructSourceImpl;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSourceImpl;
import cz.cuni.mff.odcleanstore.fusiontool.util.Closeable;
import cz.cuni.mff.odcleanstore.fusiontool.util.EnumFusionCounters;
import cz.cuni.mff.odcleanstore.fusiontool.util.MemoryProfiler;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolAppUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.ProfilingTimeCounter;
import cz.cuni.mff.odcleanstore.fusiontool.util.UriCollection;
import cz.cuni.mff.odcleanstore.fusiontool.util.UriToSameAsIterator;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriterFactory;
import cz.cuni.mff.odcleanstore.fusiontool.writers.FederatedRDFWriter;
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

/**
 * Loads and prepares all inputs for data fusion executor, executes data fusion and outputs additional metadata
 * such as canonical URIs.
 * See sample configuration files (sample-config-full.xml) for overview of all processing options.
 *
 * This class is not thread-safe.
 * @see cz.cuni.mff.odcleanstore.fusiontool.ODCSFusionToolExecutor
 */
public class ODCSFusionToolExecutorRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ODCSFusionToolExecutorRunner.class);

    /** An instance of {@link cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriterFactory}. */
    protected final CloseableRDFWriterFactory rdfWriterFactory = new CloseableRDFWriterFactory();

    /** An instance of {@link RepositoryFactory}. */
    protected final RepositoryFactory repositoryFactory;

    /** Global configuration. */
    protected final Config config;

    /** Indicates if resources to process should be discovered transitively. */
    protected final boolean isTransitive;

    /**
     * Creates new instance.
     * @param config global configuration
     */
    public ODCSFusionToolExecutorRunner(Config config) {
        this.config = config;
        isTransitive = config.getSeedResourceRestriction() != null
            && config.getSeedResourceRestriction().isTransitive();
        repositoryFactory = new RepositoryFactory(config.getParserConfig());
    }

    /**
     * Performs the actual ODCS-FusionTool task according to the given configuration.
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException general fusion error
     * @throws IOException I/O error when writing results
     * @throws ConflictResolutionException conflict resolution error
     */
    public void runFusionTool() throws ODCSFusionToolException, IOException, ConflictResolutionException {
        InputLoader inputLoader = null;
        CloseableRDFWriter rdfWriter = null;
        ProfilingTimeCounter<EnumFusionCounters> timeProfiler = ProfilingTimeCounter.createInstance(EnumFusionCounters.class, config.isProfilingOn());
        try {
            // Load source named graphs metadata
            timeProfiler.startCounter(EnumFusionCounters.META_INITIALIZATION);
            Model metadata = getMetadata(getConstructSources(config.getMetadataSources()));

            // Load & resolve owl:sameAs links
            UriMappingIterable uriMapping = getUriMapping();
            timeProfiler.stopAddCounter(EnumFusionCounters.META_INITIALIZATION);

            // Create & initialize quad loader
            timeProfiler.startCounter(EnumFusionCounters.DATA_INITIALIZATION);
            inputLoader = getInputLoader();
            inputLoader.initialize(uriMapping);
            timeProfiler.stopAddCounter(EnumFusionCounters.DATA_INITIALIZATION);

            // Initialize executor
            timeProfiler.startCounter(EnumFusionCounters.INITIALIZATION);
            ResourceDescriptionConflictResolver conflictResolver = createConflictResolver(metadata, uriMapping);
            rdfWriter = createRDFWriter();
            ODCSFusionToolExecutor executor = new ODCSFusionToolExecutor(
                    hasVirtuosoSource(config.getDataSources()),
                    config.getMaxOutputTriples(),
                    config.isProfilingOn());
            timeProfiler.stopAddCounter(EnumFusionCounters.INITIALIZATION);

            // Do the actual work
            executor.execute(inputLoader, rdfWriter, conflictResolver);

            // Write metadata
            timeProfiler.startCounter(EnumFusionCounters.META_OUTPUT_WRITING);
            writeCanonicalURIs(uriMapping, config.getCanonicalURIsOutputFile());
            writeSameAsLinks(uriMapping, config.getOutputs(), config.getPrefixes(), ValueFactoryImpl.getInstance());
            timeProfiler.stopAddCounter(EnumFusionCounters.META_OUTPUT_WRITING);

            // Print profiling information
            timeProfiler.addProfilingTimeCounter(executor.getTimeProfiler());
            printProfilingInformation(timeProfiler, executor.getMemoryProfiler());
        } finally {
            if (rdfWriter != null) {
                rdfWriter.close();
            }
            if (inputLoader != null) {
                inputLoader.close();
            }
        }
    }

    private InputLoader getInputLoader() throws IOException, ODCSFusionToolException {
        long memoryLimit = calculateMemoryLimit();
        if (config.isLocalCopyProcessing()) {
            Collection<AllTriplesLoader> allTriplesLoaders = getAllTriplesLoaders();
            return new ExternalSortingInputLoader(allTriplesLoaders, config.getTempDirectory(),
                    config.getParserConfig(), memoryLimit, config.getOutputMappedSubjectsOnly());
        } else {
            Collection<DataSource> dataSources = getDataSources();
            UriCollection seedSubjects = getSeedSubjects(dataSources, config.getSeedResourceRestriction());
            LargeCollectionFactory largeCollectionFactory = createLargeCollectionFactory();
            return (isTransitive)
                ? new TransitiveSubjectsSetInputLoader(seedSubjects, dataSources, largeCollectionFactory, config.getOutputMappedSubjectsOnly())
                : new SubjectsSetInputLoader(seedSubjects, dataSources, largeCollectionFactory, config.getOutputMappedSubjectsOnly());
        }
    }

    /**
     * Returns an initialized collection of input triple loaders.
     * @return collection of input triple loaders
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException error
     */
    protected Collection<AllTriplesLoader> getAllTriplesLoaders() throws ODCSFusionToolException {
        List<AllTriplesLoader> loaders = new ArrayList<AllTriplesLoader>(config.getDataSources().size());
        for (DataSourceConfig dataSourceConfig : config.getDataSources()) {
            try {
                AllTriplesLoader loader;
                if (dataSourceConfig.getType() == EnumDataSourceType.FILE) {
                    loader = new AllTriplesFileLoader(dataSourceConfig, config.getParserConfig());
                } else {
                    DataSource dataSource = DataSourceImpl.fromConfig(dataSourceConfig, config.getPrefixes(), repositoryFactory);
                    loader = new AllTriplesRepositoryLoader(dataSource);
                }
                loaders.add(loader);
            } catch (ODCSFusionToolException e) {
                // clean up already initialized loaders
                closeAllNoThrow(loaders);
                throw e;
            }
        }
        return loaders;
    }

    /**
     * Initializes data sources from configuration.
     * @return initialized data sources
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException I/O error
     */
    protected Collection<DataSource> getDataSources() throws ODCSFusionToolException {
        List<DataSource> dataSources = new ArrayList<DataSource>(config.getDataSources().size());
        for (DataSourceConfig dataSourceConfig : config.getDataSources()) {
            try {
                DataSource dataSource = DataSourceImpl.fromConfig(dataSourceConfig, config.getPrefixes(), repositoryFactory);
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
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException I/O error
     */
    protected Collection<ConstructSource> getConstructSources(List<ConstructSourceConfig> constructSourceConfigs)
            throws ODCSFusionToolException {
        List<ConstructSource> constructSources = new ArrayList<ConstructSource>();
        for (ConstructSourceConfig constructSourceConfig : constructSourceConfigs) {
            try {
                ConstructSource constructSource =
                        ConstructSourceImpl.fromConfig(constructSourceConfig, config.getPrefixes(), repositoryFactory);
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
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException error
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
     * Reads and resolves sameAs links and returns the result canonical URI mapping.
     * @return canonical URI mapping
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException error
     * @throws IOException I/O error
     */
    protected UriMappingIterable getUriMapping() throws ODCSFusionToolException, IOException {
        // FIXME: preference of prefixes from configuration
        Set<String> preferredURIs = getPreferredURIs(
                config.getPropertyResolutionStrategies().keySet(),
                config.getCanonicalURIsInputFile(),
                config.getPreferredCanonicalURIs());
        UriMappingIterableImpl uriMapping = new UriMappingIterableImpl(preferredURIs);

        // TODO: rework
        List<ConstructSourceConfig> repositorySameAsSourcesConfig = new ArrayList<ConstructSourceConfig>();
        List<ConstructSourceConfig> fileSameAsSourcesConfig = new ArrayList<ConstructSourceConfig>();
        for (ConstructSourceConfig sourceConfig : config.getSameAsSources()) {
            if (sourceConfig.getType() == EnumDataSourceType.FILE) {
                fileSameAsSourcesConfig.add(sourceConfig);
            } else {
                repositorySameAsSourcesConfig.add(sourceConfig);
            }
        }

        for (ConstructSourceConfig source : fileSameAsSourcesConfig) {
            SameAsLinkFileLoader loader = new SameAsLinkFileLoader(source, config.getParserConfig(), config.getSameAsLinkTypes());
            loader.loadSameAsMappings(uriMapping);
        }

        Collection<ConstructSource> repositorySameAsSources = getConstructSources(repositorySameAsSourcesConfig);
        for (ConstructSource source : repositorySameAsSources) {
            SameAsLinkRepositoryLoader loader = new SameAsLinkRepositoryLoader(source);
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
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException query error
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
            return new MapdbCollectionFactory(config.getTempDirectory());
        } else {
            return new MemoryCollectionFactory();
        }
    }

    /**
     * Creates and initializes output writer (which can be composed of multiple writers if multiple outputs are defined).
     * @return output writer
     * @throws IOException I/O error
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException configuration error
     */
    protected CloseableRDFWriter createRDFWriter() throws IOException, ODCSFusionToolException {
        List<CloseableRDFWriter> writers = new ArrayList<CloseableRDFWriter>(config.getOutputs().size());
        for (Output output : config.getOutputs()) {
            CloseableRDFWriter writer = rdfWriterFactory.createRDFWriter(output);
            writers.add(writer);
            writeNamespaceDeclarations(writer, config.getPrefixes());
        }

        return writers.size() == 1
                ? writers.get(0)
                : new FederatedRDFWriter(writers);
    }

    /**
     * Creates conflict resolver initialized according to given configuration.
     * @param metadata metadata for conflict resolution
     * @param uriMapping mapping of URIs to their canonical URI
     * @return initialized conflict resolver
     */
    protected ResourceDescriptionConflictResolver createConflictResolver(Model metadata, UriMappingIterable uriMapping) {
        SourceQualityCalculator sourceQualityCalculator = new ODCSSourceQualityCalculator(
                config.getScoreIfUnknown(),
                config.getPublisherScoreWeight());
        ResolutionFunctionRegistry registry = ConflictResolverFactory.createInitializedResolutionFunctionRegistry(
                sourceQualityCalculator,
                config.getAgreeCoefficient(),
                new DistanceMeasureImpl());

        // TODO
        //if (config.getOutputConflictsOnly()) {
        //    builder.setConflictClusterFilter(new ConflictingClusterConflictClusterFilter());
        //}
        return new ResourceDescriptionConflictResolverImpl(
                registry,
                new ConflictResolutionPolicyImpl(config.getDefaultResolutionStrategy(), config.getPropertyResolutionStrategies()),
                uriMapping,
                metadata,
                config.getResultDataURIPrefix() + ODCSInternal.QUERY_RESULT_GRAPH_URI_INFIX,
                ConfigConstants.RESOURCE_DESCRIPTION_URIS);
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
    protected void writeCanonicalURIs(UriMappingIterable uriMapping, File outputFile) throws IOException {
        if (outputFile == null) {
            return;
        }
        if (!outputFile.exists() || outputFile.canWrite()) {
            Set<String> canonicalUris = new HashSet<String>();
            for (String mappedUri : uriMapping) {
                canonicalUris.add(uriMapping.getCanonicalURI(mappedUri));
            }

            ODCSFusionToolAppUtils.ensureParentsExists(outputFile);
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
                    ODCSFusionToolAppUtils.humanReadableSize(outputStream.getByteCount())));
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
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException error when creating output
     */
    protected void writeSameAsLinks(final UriMappingIterable uriMapping, List<Output> outputs,
            Map<String, String> nsPrefixes, final ValueFactory valueFactory) throws IOException, ODCSFusionToolException {

        List<CloseableRDFWriter> writers = new LinkedList<CloseableRDFWriter>();
        try {
            // Create output writers
            for (Output output : outputs) {
                if (output.getType() != EnumOutputType.FILE || output.getParams().get(ConfigParameters.OUTPUT_SAME_AS_FILE) == null) {
                    continue;
                }

                OutputImpl sameAsOutput = new OutputImpl(EnumOutputType.FILE, output.toString() + "-sameAs");
                sameAsOutput.getParams().put(ConfigParameters.OUTPUT_PATH, output.getParams().get(ConfigParameters.OUTPUT_SAME_AS_FILE));
                sameAsOutput.getParams().put(ConfigParameters.OUTPUT_FORMAT, output.getParams().get(ConfigParameters.OUTPUT_FORMAT));
                sameAsOutput.getParams().put(ConfigParameters.OUTPUT_SPLIT_BY_MB, output.getParams().get(ConfigParameters.OUTPUT_SPLIT_BY_MB));
                CloseableRDFWriter writer = rdfWriterFactory.createRDFWriter(sameAsOutput);
                writers.add(writer);
                writer.addNamespace("owl", OWL.NAMESPACE);
                writeNamespaceDeclarations(writer, nsPrefixes);
            }
            if (writers.isEmpty()) {
                return;
            }

            for (CloseableRDFWriter writer : writers) {
                final Iterator<String> uriIterator = uriMapping.iterator();
                Iterator<Statement> sameAsTripleIterator = new UriToSameAsIterator(uriIterator, uriMapping, valueFactory);
                writer.writeQuads(sameAsTripleIterator);
            }

            int linkCounter = 0;
            for (String ignoredURI : uriMapping) {
                linkCounter++;
            }
            LOG.info("Written {} owl:sameAs links", linkCounter);
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

    private void closeAllNoThrow(List<? extends Closeable<ODCSFusionToolException>> closeables) {
        for (Closeable<ODCSFusionToolException> closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e2) {
                // ignore
            }
        }
    }

    /**
     * Calculates maximum memory limit available for data structures.
     * @return memory limit in bytes
     */
    protected long calculateMemoryLimit() {
        return Math.min(
                config.getMemoryLimit() != null ? config.getMemoryLimit() : Long.MAX_VALUE,
                (long) (ExternalSort.estimateAvailableMemory() * config.getMaxFreeMemoryUsage()));
    }

    /**
     * Prints profiling information from the given profiling time counter.
     * @param timeProfiler profiling time counter
     * @param memoryProfiler memory profiler
     */
    protected void printProfilingInformation(
            ProfilingTimeCounter<EnumFusionCounters> timeProfiler, MemoryProfiler memoryProfiler) {

        if (config.isProfilingOn()) {
            System.out.println("-- Profiling information --------");
            System.out.println("Initialization time:              " + timeProfiler.formatCounter(EnumFusionCounters.INITIALIZATION));
            System.out.println("Reading metadata & sameAs links:  " + timeProfiler.formatCounter(EnumFusionCounters.META_INITIALIZATION));
            System.out.println("Data sources initialization time: " + timeProfiler.formatCounter(EnumFusionCounters.DATA_INITIALIZATION));
            System.out.println("Quad loading time:                " + timeProfiler.formatCounter(EnumFusionCounters.QUAD_LOADING));
            System.out.println("Conflict resolution time:         " + timeProfiler.formatCounter(EnumFusionCounters.CONFLICT_RESOLUTION));
            System.out.println("Buffering time:                   " + timeProfiler.formatCounter(EnumFusionCounters.BUFFERING));
            System.out.println("Output writing time:              " + timeProfiler.formatCounter(EnumFusionCounters.OUTPUT_WRITING));
            System.out.println("Maximum recorded total memory:    " + MemoryProfiler.formatMemoryBytes(memoryProfiler.getMaxTotalMemory()));
            System.out.println("Maximum recorded used memory:     " + MemoryProfiler.formatMemoryBytes(memoryProfiler.getMaxUsedMemory()));
            System.out.println("Minimum recorded free memory:     " + MemoryProfiler.formatMemoryBytes(memoryProfiler.getMinFreeMemory()));
        }
    }
}
