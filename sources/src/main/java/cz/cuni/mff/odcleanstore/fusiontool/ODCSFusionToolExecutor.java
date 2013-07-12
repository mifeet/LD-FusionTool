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

import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunctionRegistry;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.URIMapping;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.DistanceMeasureImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.SourceQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.impl.ODCSSourceQualityCalculator;
import cz.cuni.mff.odcleanstore.fusiontool.config.Config;
import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumOutputType;
import cz.cuni.mff.odcleanstore.fusiontool.config.Output;
import cz.cuni.mff.odcleanstore.fusiontool.config.OutputImpl;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.io.CloseableRDFWriterFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.CountingOutputStream;
import cz.cuni.mff.odcleanstore.fusiontool.io.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.io.DataSourceImpl;
import cz.cuni.mff.odcleanstore.fusiontool.io.LargeCollectionFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.MapdbCollectionFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryFactory;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.BufferSubjectsCollection;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.FederatedQuadLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.FederatedSeedSubjectsLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.MetadataLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.QuadLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.RepositoryQuadLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.SameAsLinkLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.UriCollection;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.util.ConvertingIterator;
import cz.cuni.mff.odcleanstore.fusiontool.util.GenericConverter;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;
import cz.cuni.mff.odcleanstore.vocabulary.OWL;

/**
 * Fuses RDF data loaded from RDF sources using ODCS Conflict Resolution and writes the output to RDF outputs.
 * Conflict resolution includes resolution of owl:sameAs link, resolution of instance-level conflicts.
 * See sample configuration files (sample-config-full.xml) for overview of all processing options.
 * @author Jan Michelfeit
 */
public class ODCSFusionToolExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ODCSFusionToolExecutor.class);

    private static final CloseableRDFWriterFactory RDF_WRITER_FACTORY = new CloseableRDFWriterFactory();
    private static final RepositoryFactory REPOSITORY_FACTORY = new RepositoryFactory();

    /**
     * Performs the actual ODCS-FusionTool task according to the given configuration.
     * @param config global configuration
     * @throws ODCSFusionToolException general fusion error
     * @throws IOException I/O error when writing results
     * @throws ConflictResolutionException conflict resolution error
     */
    public void runFusionTool(Config config) throws ODCSFusionToolException, IOException, ConflictResolutionException {
        LargeCollectionFactory collectionFactory = createLargeCollectionFactory(config);
        QuadLoader quadLoader = null;
        UriCollection queuedSubjects = null;
        List<CloseableRDFWriter> rdfWriters = null;
        Collection<DataSource> dataSources = getDataSources(config.getDataSources(), config.getPrefixes());
        boolean hasVirtuosoSource = hasVirtuosoSource(config.getDataSources());
        try {
            // Load source named graphs metadata
            Model metadata = getMetadata(dataSources);

            // Load & resolve owl:sameAs links
            URIMappingIterable uriMapping = getURIMapping(dataSources, config);
            AlternativeURINavigator alternativeURINavigator = new AlternativeURINavigator(uriMapping);
            Set<String> resolvedCanonicalURIs = collectionFactory.createSet();

            // Get iterator over subjects of relevant triples
            UriCollection seedSubjects = getSeedSubjects(dataSources, config.getSeedResourceRestriction());
            final boolean isTransitive = config.getSeedResourceRestriction() != null;
            if (isTransitive) {
                queuedSubjects = createBufferSubjectsCollection(seedSubjects, uriMapping, collectionFactory);
                seedSubjects.close();
            } else {
                queuedSubjects = seedSubjects;
            }

            // Initialize CR
            ConflictResolver conflictResolver = createConflictResolver(config, metadata, uriMapping);

            // Initialize output writer
            rdfWriters = createRDFWriters(config.getOutputs(), config.getPrefixes());

            // Initialize triple counter
            boolean checkMaxOutputTriples = config.getMaxOutputTriples() != null && config.getMaxOutputTriples() >= 0;
            long maxOutputTriples = checkMaxOutputTriples ? config.getMaxOutputTriples() : -1;
            long outputTriples = 0;
            long inputTriples = 0;
            // Load & process relevant triples (quads) subject by subject so that we can apply CR to them
            quadLoader = createQuadLoader(dataSources, alternativeURINavigator);
            while (queuedSubjects.hasNext()) {
                String uri = queuedSubjects.next();
                String canonicalURI = uriMapping.getCanonicalURI(uri);

                if (resolvedCanonicalURIs.contains(canonicalURI)) {
                    // avoid processing a URI multiple times
                    continue;
                }
                resolvedCanonicalURIs.add(canonicalURI);

                // Load quads for the given subject
                Collection<Statement> quads = getQuads(quadLoader, canonicalURI);
                inputTriples += quads.size();

                // Resolve conflicts
                Collection<ResolvedStatement> resolvedQuads = conflictResolver.resolveConflicts(quads);
                LOG.info("Resolved {} quads for URI <{}> resulting in {} quads",
                        new Object[] { quads.size(), canonicalURI, resolvedQuads.size() });

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
                for (CloseableRDFWriter writer : rdfWriters) {
                    writer.writeCRQuads(resolvedQuads.iterator());
                }
                
                fixVirtuosoOpenedStatements(hasVirtuosoSource);
            }
            LOG.info(String.format("Processed %,d quads which were resolved to %,d output quads.", inputTriples, outputTriples));

            writeCanonicalURIs(resolvedCanonicalURIs, config.getCanonicalURIsOutputFile());
            writeSameAsLinks(uriMapping, config.getOutputs(), config.getPrefixes(), ValueFactoryImpl.getInstance());

        } finally {
            if (quadLoader != null) {
                quadLoader.close();
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

    private UriCollection createBufferSubjectsCollection(UriCollection seedSubjects, URIMapping uriMapping,
            LargeCollectionFactory collectionFactory) throws ODCSFusionToolException {
        Set<String> buffer = collectionFactory.<String>createSet();
        UriCollection queuedSubjects = new BufferSubjectsCollection(buffer);
        while (seedSubjects.hasNext()) {
            String canonicalURI = uriMapping.getCanonicalURI(seedSubjects.next());
            queuedSubjects.add(canonicalURI); // only store canonical URIs to save space
        }
        if (LOG.isDebugEnabled()) {
            // only when debug is enabled, this may be expensive when using file cache
            LOG.debug(String.format("ODCS-FusionTool: loaded %,d seed resources", buffer.size()));
        } 
        return queuedSubjects;
    }

    private Collection<DataSource> getDataSources(List<DataSourceConfig> config, Map<String, String> prefixes)
            throws ODCSFusionToolException {
        List<DataSource> dataSources = new ArrayList<DataSource>();
        for (DataSourceConfig dataSourceConfig : config) {
            try {
                DataSource dataSource = DataSourceImpl.fromConfig(dataSourceConfig, prefixes, REPOSITORY_FACTORY);
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
    
    private Model getMetadata(Collection<DataSource> dataSources) throws ODCSFusionToolException {
        Model metadata = new TreeModel();
        for (DataSource source : dataSources) {
            MetadataLoader loader = new MetadataLoader(source);
            loader.loadNamedGraphsMetadata(metadata);
        }
        return metadata;
    }
    
    private URIMappingIterable getURIMapping(Collection<DataSource> dataSources, Config config)
            throws ODCSFusionToolException, IOException {
        Set<String> preferredURIs = getPreferredURIs(
                config.getPropertyResolutionStrategies().keySet(), 
                config.getCanonicalURIsInputFile());
        URIMappingIterableImpl uriMapping = new URIMappingIterableImpl(preferredURIs);
        for (DataSource source : dataSources) {
            SameAsLinkLoader loader = new SameAsLinkLoader(source);
            loader.loadSameAsMappings(uriMapping);
        }
        return uriMapping;
    }
    

    private UriCollection getSeedSubjects(Collection<DataSource> dataSources, SparqlRestriction seedResourceRestriction)
            throws ODCSFusionToolException { 
        FederatedSeedSubjectsLoader loader = new FederatedSeedSubjectsLoader(dataSources);
        return loader.getTripleSubjectsCollection(seedResourceRestriction);
    }
    
    private Collection<Statement> getQuads(QuadLoader quadLoader, String canonicalURI) throws ODCSFusionToolException {
        Collection<Statement> quads = new ArrayList<Statement>();
        quadLoader.loadQuadsForURI(canonicalURI, quads);
        return quads;
    }
    
    private LargeCollectionFactory createLargeCollectionFactory(Config config) throws IOException {
        if (config.getEnableFileCache()) {
            return new MapdbCollectionFactory(ODCSFusionToolUtils.getCacheDirectory());
        } else {
            return new LargeCollectionFactory() {
                @Override
                public <T> Set<T> createSet() {
                    return new HashSet<T>();
                }

                @Override
                public void close() throws IOException {
                    // do nothing
                }
            };
        }
    }
    
    private QuadLoader createQuadLoader(Collection<DataSource> dataSources, AlternativeURINavigator alternativeURINavigator) {
        if (dataSources.size() == 1) {
            return new RepositoryQuadLoader(dataSources.iterator().next(), alternativeURINavigator);
        } else {
            return new FederatedQuadLoader(dataSources, alternativeURINavigator);
        }
    }
    
    private static List<CloseableRDFWriter> createRDFWriters(List<Output> outputs, Map<String, String> nsPrefixes)
            throws IOException, ODCSFusionToolException {
        List<CloseableRDFWriter> writers = new LinkedList<CloseableRDFWriter>();
        for (Output output : outputs) {
            CloseableRDFWriter writer = RDF_WRITER_FACTORY.createRDFWriter(output);
            writers.add(writer);
            writeNamespaceDeclarations(writer, nsPrefixes);
        }
        return writers;
    }

    private static ConflictResolver createConflictResolver(
            Config config, Model metadata, URIMappingIterable uriMapping) {

        SourceQualityCalculator sourceConfidence = new ODCSSourceQualityCalculator(
                config.getScoreIfUnknown(), 
                config.getPublisherScoreWeight());
        ResolutionFunctionRegistry registry = ConflictResolverFactory.createInitializedResolutionFunctionRegistry(
                sourceConfidence, 
                config.getAgreeCoeficient(),
                new DistanceMeasureImpl());

        ConflictResolver conflictResolver = ConflictResolverFactory.configureResolver()
                .setResolutionFunctionRegistry(registry)
                .setResolvedGraphsURIPrefix(config.getResultDataURIPrefix() + ODCSInternal.queryResultGraphUriInfix)
                .setMetadata(metadata)
                .setURIMapping(uriMapping)
                .setDefaultResolutionStrategy(config.getDefaultResolutionStrategy())
                .setPropertyResolutionStrategies(config.getPropertyResolutionStrategies())
                .create();
        return conflictResolver;
    }

    private static Set<String> getPreferredURIs(Set<URI> settingsPreferredURIs, File canonicalURIsInputFile)
            throws IOException {
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
                LOG.error("Cannot read canonical URIs from '{}'", canonicalURIsInputFile.getPath());
                // Intentionally do not throw an exception
            }
        }

        return preferredURIs;
    }

    private static void addDiscoveredObjects(UriCollection queuedSubjects, Collection<ResolvedStatement
            > resolvedStatements,
            URIMappingIterable uriMapping, Set<String> resolvedCanonicalURIs) {
        for (ResolvedStatement resolvedStatement : resolvedStatements) {
            String uri = ODCSFusionToolUtils.getNodeURI(resolvedStatement.getStatement().getObject());
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
    }
    
    private static void writeNamespaceDeclarations(CloseableRDFWriter writer, Map<String, String> nsPrefixes) throws IOException {
        for (Map.Entry<String, String> entry : nsPrefixes.entrySet()) {
            writer.addNamespace(entry.getKey(), entry.getValue());
        }
    }

    private static void writeCanonicalURIs(Set<String> resolvedCanonicalURIs, File outputFile) throws IOException {
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

    private static void writeSameAsLinks(final URIMappingIterable uriMapping, List<Output> outputs,
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
                writer.addNamespace("owl", OWL.getURI());
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

    private static boolean hasVirtuosoSource(List<DataSourceConfig> sources) {
        for (DataSourceConfig sourceConfig : sources) {
            if (sourceConfig.getType() == EnumDataSourceType.VIRTUOSO) {
                return true;
            }
        }
        return false;
    }
    
    private static void fixVirtuosoOpenedStatements(boolean hasVirtuosoSource) {
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
}