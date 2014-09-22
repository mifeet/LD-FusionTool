package cz.cuni.mff.odcleanstore.fusiontool;

import com.google.code.externalsorting.ExternalSort;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunctionRegistry;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictResolutionPolicyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.DistanceMeasureImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.SourceQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.impl.DecidingConflictFQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.impl.ODCSSourceQualityCalculator;
import cz.cuni.mff.odcleanstore.fusiontool.config.*;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.NestedResourceDescriptionQualityCalculatorImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.ResourceDescriptionConflictResolverImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
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
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.FederatedResourceDescriptionFilter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.MappedResourceFilter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.RequiredClassFilter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.ResourceDescriptionFilter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.metadata.MetadataLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.sameas.SameAsLinkFileLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.sameas.SameAsLinkRepositoryLoader;
import cz.cuni.mff.odcleanstore.fusiontool.source.ConstructSource;
import cz.cuni.mff.odcleanstore.fusiontool.source.ConstructSourceImpl;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSourceImpl;
import cz.cuni.mff.odcleanstore.fusiontool.util.*;
import cz.cuni.mff.odcleanstore.fusiontool.writers.*;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;
import org.openrdf.model.Model;
import org.openrdf.model.URI;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Loads and prepares all inputs for data fusion executor, executes data fusion and outputs additional metadata
 * such as canonical URIs.
 * See sample configuration files (sample-config-full.xml) for overview of all processing options.
 * <p/>
 * This class is not thread-safe.
 * @see LDFusionToolExecutor
 */
public class LDFusionToolComponentFactory implements FusionComponentFactory {
    private static final Logger LOG = LoggerFactory.getLogger(LDFusionToolComponentFactory.class);

    private static final CloseableRDFWriterFactory rdfWriterFactory = new CloseableRDFWriterFactory();

    /** An instance of {@link cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryFactory}. */
    protected final RepositoryFactory repositoryFactory;

    /** Global configuration. */
    protected final Config config;

    /** Indicates if resources to process should be discovered transitively. */
    protected final boolean isTransitive;

    private ProfilingTimeCounter<EnumFusionCounters> executorTimeProfiler;

    private MemoryProfiler executorMemoryProfiler;

    /**
     * Creates new instance.
     * @param config global configuration
     */
    public LDFusionToolComponentFactory(Config config) {
        this.config = config;
        isTransitive = false; // TODO
        repositoryFactory = new RepositoryFactory(config.getParserConfig());
        executorTimeProfiler = ProfilingTimeCounter.createInstance(EnumFusionCounters.class, config.isProfilingOn());
        executorMemoryProfiler = MemoryProfiler.createInstance(config.isProfilingOn());
    }

    @Override
    public LDFusionToolExecutor getExecutor(UriMappingIterable uriMapping) {
        return new LDFusionToolExecutor(
                hasVirtuosoSource(config.getDataSources()),
                config.getMaxOutputTriples(),
                getInputFilter(uriMapping),
                executorTimeProfiler,
                executorMemoryProfiler
        );
    }

    @Override
    public InputLoader getInputLoader() throws IOException, LDFusionToolException {
        long memoryLimit = calculateMemoryLimit();
        if (config.isLocalCopyProcessing()) {
            Collection<AllTriplesLoader> allTriplesLoaders = getAllTriplesLoaders();
            return new ExternalSortingInputLoader(allTriplesLoaders,
                    LDFusionToolUtils.getResourceDescriptionProperties(config),
                    config.getTempDirectory(),
                    config.getParserConfig(),
                    memoryLimit);
        } else {
            Collection<DataSource> dataSources = getDataSources();
            SparqlRestriction seedResourceDescription = getSeedResourceRestriction();
            UriCollection seedSubjects = getSeedSubjects(dataSources, seedResourceDescription);
            LargeCollectionFactory largeCollectionFactory = createLargeCollectionFactory();
            return (isTransitive)
                    ? new TransitiveSubjectsSetInputLoader(seedSubjects, dataSources, largeCollectionFactory, config.getOutputMappedSubjectsOnly())
                    : new SubjectsSetInputLoader(seedSubjects, dataSources, largeCollectionFactory, config.getOutputMappedSubjectsOnly());
        }
    }

    private SparqlRestriction getSeedResourceRestriction() {
        if (config.getRequiredClassOfProcessedResources() == null) {
            return null;
        } else {
            final String varPrefix = "be3611ab1a_"; // some unique prefix
            return new SparqlRestrictionImpl(String.format("?%sr rdf:type <%s>", varPrefix, config.getRequiredClassOfProcessedResources().stringValue()),
                    varPrefix + "r");
        }
    }

    protected ResourceDescriptionFilter getInputFilter(UriMappingIterable uriMapping) {
        List<ResourceDescriptionFilter> inputFilters = new ArrayList<>();
        if (config.getRequiredClassOfProcessedResources() != null) {
            inputFilters.add(new RequiredClassFilter(uriMapping, config.getRequiredClassOfProcessedResources()));
        }
        if (config.getOutputMappedSubjectsOnly()) {
            inputFilters.add(new MappedResourceFilter(new AlternativeUriNavigator(uriMapping)));
        }

        return FederatedResourceDescriptionFilter.fromList(inputFilters);
    }

    /**
     * Returns an initialized collection of input triple loaders.
     * @return collection of input triple loaders
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException error
     */
    protected Collection<AllTriplesLoader> getAllTriplesLoaders() throws LDFusionToolException {
        List<AllTriplesLoader> loaders = new ArrayList<>(config.getDataSources().size());
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
            } catch (LDFusionToolException e) {
                // clean up already initialized loaders
                for (AllTriplesLoader loader : loaders) {
                    LDFusionToolUtils.closeQuietly(loader);
                }
                throw e;
            }
        }
        return loaders;
    }

    /**
     * Initializes data sources from configuration.
     * @return initialized data sources
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException I/O error
     */
    protected Collection<DataSource> getDataSources() throws LDFusionToolException {
        List<DataSource> dataSources = new ArrayList<>(config.getDataSources().size());
        for (DataSourceConfig dataSourceConfig : config.getDataSources()) {
            try {
                DataSource dataSource = DataSourceImpl.fromConfig(dataSourceConfig, config.getPrefixes(), repositoryFactory);
                dataSources.add(dataSource);
            } catch (LDFusionToolException e) {
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
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException I/O error
     */
    protected Collection<ConstructSource> getConstructSources(List<ConstructSourceConfig> constructSourceConfigs)
            throws LDFusionToolException {
        List<ConstructSource> constructSources = new ArrayList<>();
        for (ConstructSourceConfig constructSourceConfig : constructSourceConfigs) {
            try {
                ConstructSource constructSource =
                        ConstructSourceImpl.fromConfig(constructSourceConfig, config.getPrefixes(), repositoryFactory);
                constructSources.add(constructSource);
            } catch (LDFusionToolException e) {
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
     * @return metadata for conflict resolution
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException error
     */
    @Override
    public Model getMetadata() throws LDFusionToolException {
        Collection<ConstructSource> metadataSources = getConstructSources(config.getMetadataSources());
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
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException error
     * @throws java.io.IOException I/O error
     */
    @Override
    public UriMappingIterable getUriMapping() throws LDFusionToolException, IOException {
        // FIXME: preference of prefixes from configuration
        Set<String> preferredURIs = getPreferredURIs(
                config.getPropertyResolutionStrategies().keySet(),
                config.getCanonicalURIsInputFile(),
                config.getPreferredCanonicalURIs());
        UriMappingIterableImpl uriMapping = new UriMappingIterableImpl(preferredURIs);

        // TODO: rework
        List<ConstructSourceConfig> repositorySameAsSourcesConfig = new ArrayList<>();
        List<ConstructSourceConfig> fileSameAsSourcesConfig = new ArrayList<>();
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
     * Returns set of URIs preferred for canonical URIs.
     * The URIs are loaded from canonicalURIsInputFile if given and URIs present in settingsPreferredURIs are added.
     * @param settingsPreferredURIs URIs occurring on fusion tool configuration
     * @param canonicalURIsInputFile file with canonical URIs to be loaded; can be null
     * @param preferredCanonicalURIs default set of preferred canonical URIs
     * @return set of URIs preferred for canonical URIs
     * @throws java.io.IOException error reading canonical URIs from file
     */
    protected static Set<String> getPreferredURIs(
            Set<URI> settingsPreferredURIs, File canonicalURIsInputFile,
            Collection<String> preferredCanonicalURIs) throws IOException {

        Set<String> preferredURIs = new HashSet<>(settingsPreferredURIs.size());
        for (URI uri : settingsPreferredURIs) {
            preferredURIs.add(uri.stringValue());
        }
        if (canonicalURIsInputFile != null) {
            new CanonicalUriFileHelper().readCanonicalUris(canonicalURIsInputFile, preferredURIs);
        }
        preferredURIs.addAll(preferredCanonicalURIs);

        return preferredURIs;
    }

    /**
     * Returns collections of seed subjects, i.e. the initial URIs for which
     * corresponding quads are loaded and resolved.
     * @param dataSources initialized data sources
     * @param seedResourceRestriction SPARQL restriction on URI resources which are initially loaded and processed
     * or null to iterate all subjects
     * @return collection of seed subject URIs
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException query error
     */
    protected UriCollection getSeedSubjects(Collection<DataSource> dataSources, SparqlRestriction seedResourceRestriction)
            throws LDFusionToolException {
        FederatedSeedSubjectsLoader loader = new FederatedSeedSubjectsLoader(dataSources);
        return loader.getTripleSubjectsCollection(seedResourceRestriction);
    }

    /**
     * Creates factory object for large collections depending on configuration.
     * If cache is enabled, the collection is backed by a file, otherwise kept in memory.
     * @return factory for large collections
     * @throws java.io.IOException I/O error
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
     * @throws java.io.IOException I/O error
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException configuration error
     */
    @Override
    public CloseableRDFWriter getRDFWriter() throws IOException, LDFusionToolException {
        List<CloseableRDFWriter> writers = new ArrayList<>(config.getOutputs().size());
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
     * Writes namespace declarations to the given output writer.
     * @param writer output writer
     * @param nsPrefixes map of namespace prefixes
     * @throws java.io.IOException I/O error
     */
    protected void writeNamespaceDeclarations(CloseableRDFWriter writer, Map<String, String> nsPrefixes) throws IOException {
        for (Map.Entry<String, String> entry : nsPrefixes.entrySet()) {
            writer.addNamespace(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Creates conflict resolver initialized according to given configuration.
     * @param metadata metadata for conflict resolution
     * @param uriMapping mapping of URIs to their canonical URI
     * @return initialized conflict resolver
     */
    @Override
    public ResourceDescriptionConflictResolver getConflictResolver(Model metadata, UriMappingIterable uriMapping) {
        DistanceMeasureImpl distanceMeasure = new DistanceMeasureImpl();
        SourceQualityCalculator sourceQualityCalculator = new ODCSSourceQualityCalculator(
                config.getScoreIfUnknown(),
                config.getPublisherScoreWeight());
        ResolutionFunctionRegistry registry = ConflictResolverFactory.createInitializedResolutionFunctionRegistry(
                sourceQualityCalculator,
                config.getAgreeCoefficient(),
                distanceMeasure);
        NestedResourceDescriptionQualityCalculatorImpl nestedResourceDescriptionQualityCalculator = new NestedResourceDescriptionQualityCalculatorImpl(
                new DecidingConflictFQualityCalculator(sourceQualityCalculator, config.getAgreeCoefficient(), distanceMeasure));

        // TODO
        //if (config.getOutputConflictsOnly()) {
        //    builder.setConflictClusterFilter(new ConflictingClusterConflictClusterFilter());
        //}
        return new ResourceDescriptionConflictResolverImpl(
                registry,
                new ConflictResolutionPolicyImpl(config.getDefaultResolutionStrategy(), config.getPropertyResolutionStrategies()),
                uriMapping,
                metadata,
                config.getResultDataURIPrefix() + ODCSInternal.QUERY_RESULT_GRAPH_URI_INFIX + "/",
                nestedResourceDescriptionQualityCalculator
        );
    }

    @Override
    public UriMappingWriter getCanonicalUriWriter(UriMappingIterable uriMapping) throws IOException {
        return new CanonicalUriFileWriter(config.getCanonicalURIsOutputFile());
    }

    /**
     * Returns writer for owl:sameAs links according to the given URI mapping to given outputs.
     * @throws java.io.IOException I/O error
     */
    @Override
    public UriMappingWriter getSameAsLinksWriter() throws IOException {
        return new SameAsLinkWriter(config.getOutputs(), config.getPrefixes());
    }

    /**
     * Indicates whether a source of type {@link cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType#VIRTUOSO} is present in the given data sources.
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
     * Calculates maximum memory limit available for data structures.
     * @return memory limit in bytes
     */
    protected long calculateMemoryLimit() {
        return Math.min(
                config.getMemoryLimit() != null ? config.getMemoryLimit() : Long.MAX_VALUE,
                (long) (ExternalSort.estimateAvailableMemory() * config.getMaxFreeMemoryUsage()));
    }

    public ProfilingTimeCounter<EnumFusionCounters> getExecutorTimeProfiler() {
        return executorTimeProfiler;
    }

    public MemoryProfiler getExecutorMemoryProfiler() {
        return executorMemoryProfiler;
    }
}
