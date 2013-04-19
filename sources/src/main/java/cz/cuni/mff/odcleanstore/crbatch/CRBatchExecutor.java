/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.configuration.ConflictResolutionConfig;
import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;
import cz.cuni.mff.odcleanstore.conflictresolution.CRQuad;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.URIMapping;
import cz.cuni.mff.odcleanstore.crbatch.config.Config;
import cz.cuni.mff.odcleanstore.crbatch.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.crbatch.config.Output;
import cz.cuni.mff.odcleanstore.crbatch.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.io.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.crbatch.io.CloseableRDFWriterFactory;
import cz.cuni.mff.odcleanstore.crbatch.io.CountingOutputStream;
import cz.cuni.mff.odcleanstore.crbatch.io.EnumSerializationFormat;
import cz.cuni.mff.odcleanstore.crbatch.io.LargeCollectionFactory;
import cz.cuni.mff.odcleanstore.crbatch.io.MapdbCollectionFactory;
import cz.cuni.mff.odcleanstore.crbatch.loaders.BufferSubjectsCollection;
import cz.cuni.mff.odcleanstore.crbatch.loaders.FederatedQuadLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.FederatedSeedSubjectsLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.NamedGraphMetadataLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.QuadLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.RepositoryQuadLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.SameAsLinkLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.UriCollection;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.URIMappingIterableImpl;
import cz.cuni.mff.odcleanstore.crbatch.util.CRBatchUtils;
import cz.cuni.mff.odcleanstore.crbatch.util.ConvertingIterator;
import cz.cuni.mff.odcleanstore.crbatch.util.GenericConverter;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;
import cz.cuni.mff.odcleanstore.vocabulary.OWL;

/**
 * Processed RDF data loaded from database with Conflict Resolution and writes the output to a file.
 * Conflict resolution includes resolution of owl:sameAs link, resolution of instance-level conflicts;
 * quality & provenance information is not written to the output (as of now).
 * @author Jan Michelfeit
 */
public class CRBatchExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(CRBatchExecutor.class);

    private static final CloseableRDFWriterFactory RDF_WRITER_FACTORY = new CloseableRDFWriterFactory();

    /**
     * Performs the actual CR-batch task according to the given configuration.
     * @param config global configuration
     * @throws CRBatchException general batch error
     * @throws IOException I/O error when writing results
     * @throws ConflictResolutionException conflict resolution error
     */
    public void runCRBatch(Config config) throws CRBatchException, IOException, ConflictResolutionException {
        LargeCollectionFactory collectionFactory = createLargeCollectionFactory(config);
        QuadLoader quadLoader = null;
        UriCollection queuedSubjects = null;
        List<CloseableRDFWriter> rdfWriters = null;
        Collection<DataSource> dataSources = getDataSources(config.getDataSources(), config.getPrefixes());
        try {
            // Load source named graphs metadata
            NamedGraphMetadataMap namedGraphsMetadata = getMetadata(dataSources);

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
            ConflictResolver conflictResolver = createConflictResolver(config, namedGraphsMetadata, uriMapping);

            // Initialize output writer
            rdfWriters = createRDFWriters(config.getOutputs(), config.getPrefixes());

            // Initialize triple counter
            boolean checkMaxOutputTriples = config.getMaxOutputTriples() != null && config.getMaxOutputTriples() >= 0;
            long maxOutputTriples = checkMaxOutputTriples ? config.getMaxOutputTriples() : -1;
            long outputTriples = 0;

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

                // Resolve conflicts
                Collection<CRQuad> resolvedQuads = conflictResolver.resolveConflicts(quads);
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
                    Iterator<Statement> resolvedTriplesIterator = crQuadsAsStatements(resolvedQuads.iterator());
                    writer.write(resolvedTriplesIterator);
                }
            }
            LOG.info(String.format("Written %,d resolved quads", outputTriples));

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
        }
    }

    private UriCollection createBufferSubjectsCollection(UriCollection seedSubjects, URIMapping uriMapping,
            LargeCollectionFactory collectionFactory) throws CRBatchException {
        Set<String> buffer = collectionFactory.<String>createSet();
        UriCollection queuedSubjects = new BufferSubjectsCollection(buffer);
        while (seedSubjects.hasNext()) {
            String canonicalURI = uriMapping.getCanonicalURI(seedSubjects.next());
            queuedSubjects.add(canonicalURI); // only store canonical URIs to save space
        }
        if (LOG.isDebugEnabled()) {
            // only when debug is enabled, this may be expensive when using file cache
            LOG.debug(String.format("CR-batch: loaded %,d seed resources", buffer.size()));
        } 
        return queuedSubjects;
    }

    private Collection<DataSource> getDataSources(List<DataSourceConfig> config, Map<String, String> prefixes)
            throws CRBatchException {
        List<DataSource> dataSources = new ArrayList<DataSource>();
        RepositoryFactory repositoryFactory = new RepositoryFactory();
        for (DataSourceConfig dataSourceConfig : config) {
            try {
                DataSource dataSource = DataSourceImpl.fromConfig(dataSourceConfig, prefixes, repositoryFactory);
                dataSources.add(dataSource);
            } catch (CRBatchException e) {
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
    
    private NamedGraphMetadataMap getMetadata(Collection<DataSource> dataSources) throws CRBatchException {
        NamedGraphMetadataMap metadata = new NamedGraphMetadataMap();
        for (DataSource source : dataSources) {
            NamedGraphMetadataLoader loader = new NamedGraphMetadataLoader(source);
            loader.loadNamedGraphsMetadata(metadata);
        }
        return metadata;
    }
    
    private URIMappingIterable getURIMapping(Collection<DataSource> dataSources, Config config)
            throws CRBatchException, IOException {
        Set<String> preferredURIs = getPreferredURIs(config.getAggregationSpec(), config.getCanonicalURIsInputFile());
        URIMappingIterableImpl uriMapping = new URIMappingIterableImpl(preferredURIs);
        for (DataSource source : dataSources) {
            SameAsLinkLoader loader = new SameAsLinkLoader(source);
            loader.loadSameAsMappings(uriMapping);
        }
        return uriMapping;
    }
    

    private UriCollection getSeedSubjects(Collection<DataSource> dataSources, SparqlRestriction seedResourceRestriction)
            throws CRBatchException { 
        FederatedSeedSubjectsLoader loader = new FederatedSeedSubjectsLoader(dataSources);
        return loader.getTripleSubjectsCollection(seedResourceRestriction);
    }
    
    private Collection<Statement> getQuads(QuadLoader quadLoader, String canonicalURI) throws CRBatchException {
        Collection<Statement> quads = new ArrayList<Statement>();
        quadLoader.loadQuadsForURI(canonicalURI, quads);
        return quads;
    }
    
    private LargeCollectionFactory createLargeCollectionFactory(Config config) throws IOException {
        if (config.getEnableFileCache()) {
            return new MapdbCollectionFactory(CRBatchUtils.getCacheDirectory());
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
            throws IOException {
        List<CloseableRDFWriter> writers = new LinkedList<CloseableRDFWriter>();
        for (Output output : outputs) {
            CloseableRDFWriter writer = createOutputWriter(
                    output.getFileLocation(),
                    output.getFormat(),
                    output.getSplitByBytes());
            writers.add(writer);
            writeNamespaceDeclarations(writer, nsPrefixes);
        }
        return writers;
    }

    private static CloseableRDFWriter createOutputWriter(File file, EnumSerializationFormat outputFormat, Long splitByBytes)
            throws IOException {

        CRBatchUtils.ensureParentsExists(file);
        if (splitByBytes == null) {
            return RDF_WRITER_FACTORY.createRDFWriter(outputFormat, new FileOutputStream(file));
        } else {
            return RDF_WRITER_FACTORY.createSplittingRDFWriter(outputFormat, file, splitByBytes);
        }
    }
    
    private static ConflictResolver createConflictResolver(
            Config config, NamedGraphMetadataMap namedGraphsMetadata, URIMappingIterable uriMapping) {

        ConflictResolutionConfig crConfig = new ConflictResolutionConfig(
                config.getAgreeCoeficient(),
                config.getScoreIfUnknown(),
                config.getNamedGraphScoreWeight(),
                config.getPublisherScoreWeight(),
                config.getMaxDateDifference());

        ConflictResolverFactory conflictResolverFactory = new ConflictResolverFactory(
                config.getResultDataURIPrefix() + ODCSInternal.queryResultGraphUriInfix,
                crConfig,
                new AggregationSpec());

        ConflictResolver conflictResolver = conflictResolverFactory.createResolver(
                config.getAggregationSpec(),
                namedGraphsMetadata,
                uriMapping);

        return conflictResolver;
    }

    private static Set<String> getPreferredURIs(AggregationSpec aggregationSpec, File canonicalURIsInputFile) throws IOException {
        Set<String> preferredURIs = getSettingsPreferredURIs(aggregationSpec);
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

    private static Set<String> getSettingsPreferredURIs(AggregationSpec aggregationSpec) {
        Set<String> aggregationProperties = aggregationSpec.getPropertyAggregations() == null
                ? Collections.<String>emptySet()
                : aggregationSpec.getPropertyAggregations().keySet();
        Set<String> multivalueProperties = aggregationSpec.getPropertyMultivalue() == null
                ? Collections.<String>emptySet()
                : aggregationSpec.getPropertyMultivalue().keySet();

        Set<String> preferredURIs = new HashSet<String>(
                aggregationProperties.size() + multivalueProperties.size());
        preferredURIs.addAll(aggregationProperties);
        preferredURIs.addAll(multivalueProperties);
        return preferredURIs;
    }
    
    private void addDiscoveredObjects(UriCollection queuedSubjects, Collection<CRQuad> resolvedQuads,
            URIMappingIterable uriMapping, Set<String> resolvedCanonicalURIs) {
        for (CRQuad crQuad : resolvedQuads) {
            String uri = CRBatchUtils.getNodeURI(crQuad.getQuad().getObject());
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
    
    private static Iterator<Statement> crQuadsAsStatements(Iterator<CRQuad> crQuads) {
        return ConvertingIterator.decorate(crQuads, new GenericConverter<CRQuad, Statement>() {
            @Override
            public Statement convert(CRQuad object) {
                return object.getQuad();
            }
        });
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
            CRBatchUtils.ensureParentsExists(outputFile);
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
                    CRBatchUtils.humanReadableSize(outputStream.getByteCount())));
        } else {
            LOG.error("Cannot write canonical URIs to '{}'", outputFile.getPath());
            // Intentionally do not throw an exception
        }
    }

    private static void writeSameAsLinks(final URIMappingIterable uriMapping, List<Output> outputs,
            Map<String, String> nsPrefixes, final ValueFactory valueFactory) throws IOException {

        List<CloseableRDFWriter> writers = new LinkedList<CloseableRDFWriter>();
        try {
            // Create output writers
            for (Output output : outputs) {
                if (output.getSameAsFileLocation() == null) {
                    continue;
                }
                CloseableRDFWriter writer = createOutputWriter(
                        output.getSameAsFileLocation(),
                        output.getFormat(),
                        output.getSplitByBytes());
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
                writer.write(sameAsTripleIterator);
            }

            LOG.info("Written owl:sameAs links");
        } finally {
            for (CloseableRDFWriter writer : writers) {
                writer.close();
            }
        }
    }
}
