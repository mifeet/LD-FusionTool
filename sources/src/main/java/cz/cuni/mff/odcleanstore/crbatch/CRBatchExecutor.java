/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import cz.cuni.mff.odcleanstore.configuration.ConflictResolutionConfig;
import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;
import cz.cuni.mff.odcleanstore.conflictresolution.CRQuad;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.crbatch.config.Config;
import cz.cuni.mff.odcleanstore.crbatch.config.Output;
import cz.cuni.mff.odcleanstore.crbatch.config.QueryConfig;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.io.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.crbatch.io.EnumOutputFormat;
import cz.cuni.mff.odcleanstore.crbatch.io.IncrementalN3Writer;
import cz.cuni.mff.odcleanstore.crbatch.io.IncrementalRdfXmlWriter;
import cz.cuni.mff.odcleanstore.crbatch.loaders.NamedGraphLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.NodeIterator;
import cz.cuni.mff.odcleanstore.crbatch.loaders.QuadLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.SameAsLinkLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.SeedSubjectsLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.TransitiveSubjectsIterator;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.crbatch.util.ConvertingIterator;
import cz.cuni.mff.odcleanstore.crbatch.util.GenericConverter;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;
import cz.cuni.mff.odcleanstore.vocabulary.OWL;
import de.fuberlin.wiwiss.ng4j.Quad;

/**
 * Processed RDF data loaded from database with Conflict Resolution and writes the output to a file.
 * Conflict resolution includes resolution of owl:sameAs link, resolution of instance-level conflicts;
 * quality & provenance information is not written to the output (as of now).
 * @author Jan Michelfeit
 */
public class CRBatchExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(CRBatchExecutor.class);

    /**
     * Performs the actual CR-batch task according to the given configuration.
     * @param config global configuration
     * @throws CRBatchException general batch error
     * @throws IOException I/O error when writing results
     * @throws ConflictResolutionException conflict resolution error
     */
    public void runCRBatch(Config config) throws CRBatchException, IOException, ConflictResolutionException {
        ConnectionFactory connectionFactory = new ConnectionFactory(config);

        // Load source named graphs metadata
        NamedGraphLoader graphLoader = new NamedGraphLoader(connectionFactory, (QueryConfig) config);
        NamedGraphMetadataMap namedGraphsMetadata = graphLoader.getNamedGraphs();

        // Load & resolve owl:sameAs links
        SameAsLinkLoader sameAsLoader = new SameAsLinkLoader(connectionFactory, (QueryConfig) config);
        Set<String> preferredURIs = getPreferredURIs(config.getAggregationSpec(), config.getCanonicalURIsInputFile());
        URIMappingIterable uriMapping = sameAsLoader.getSameAsMappings(preferredURIs);
        AlternativeURINavigator alternativeURINavigator = new AlternativeURINavigator(uriMapping);
        HashSet<String> resolvedCanonicalURIs = new HashSet<String>();
        
        // Get iterator over subjects of relevant triples
        SeedSubjectsLoader tripleSubjectsLoader = new SeedSubjectsLoader(connectionFactory, (QueryConfig) config);
        NodeIterator subjectsIterator;
        TransitiveSubjectsIterator transitiveSubjectIterator;
        if (config.getSeedResourceRestriction() != null) {
            NodeIterator seedResourceIterator = tripleSubjectsLoader.getTripleSubjectIterator();
            transitiveSubjectIterator = TransitiveSubjectsIterator.createTransitiveSubjectsIterator(seedResourceIterator);
            subjectsIterator = transitiveSubjectIterator;
        } else {
            transitiveSubjectIterator = null;
            subjectsIterator = tripleSubjectsLoader.getTripleSubjectIterator();
        }

        // Initialize CR
        ConflictResolver conflictResolver = createConflictResolver(config, namedGraphsMetadata, uriMapping);

        // Initialize output writer
        // TODO: writing could be more (esp. memory) efficient by avoiding models and serializing manually
        List<CloseableRDFWriter> rdfWriters = createRDFWriters(config.getOutputs(), config.getPrefixes());

        // Load & process relevant triples (quads) subject by subject so that we can apply CR to them
        QuadLoader quadLoader = new QuadLoader(connectionFactory, config, alternativeURINavigator);
        try {
            while (subjectsIterator.hasNext()) {
                String uri = getNodeURI(subjectsIterator.next());
                if (uri == null) {
                    continue;
                }
                String canonicalURI = uriMapping.getCanonicalURI(uri);
                if (resolvedCanonicalURIs.contains(canonicalURI)) {
                    // avoid processing a URI multiple times
                    continue;
                }
                resolvedCanonicalURIs.add(canonicalURI);

                // Load quads for the given subject
                Collection<Quad> quads = quadLoader.getQuadsForURI(canonicalURI);

                // Resolve conflicts
                Collection<CRQuad> resolvedQuads = conflictResolver.resolveConflicts(quads);
                LOG.info("Resolved {} quads for URI <{}> resulting in {} quads",
                        new Object[] { quads.size(), canonicalURI, resolvedQuads.size() });

                // Add objects filtered by CR for traversal
                if (transitiveSubjectIterator != null) {
                    transitiveSubjectIterator.addObjectsFromCRQuads(resolvedQuads);
                }

                // Write result to output
                for (CloseableRDFWriter writer : rdfWriters) {
                    Iterator<Triple> resolvedTriplesIterator = crQuadsAsTriples(resolvedQuads.iterator());
                    writer.write(resolvedTriplesIterator);
                }
            }
        } finally {
            quadLoader.close();
            for (CloseableRDFWriter writer : rdfWriters) {
                writer.close();
            }
        }

        writeCanonicalURIs(resolvedCanonicalURIs, config.getCanonicalURIsOutputFile());
        writeSameAsLinks(uriMapping, config.getOutputs(), config.getPrefixes());
    }

    private String getNodeURI(Node node) {
        if (node.isURI()) {
            return node.getURI();
        } else if (node.isBlank()) {
            return ODCSUtils.getVirtuosoURIForBlankNode(node);
        } else {
            return null;
        }
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

    private static List<CloseableRDFWriter> createRDFWriters(List<Output> outputs, Map<String, String> nsPrefixes)
            throws IOException {
        List<CloseableRDFWriter> writers = new LinkedList<CloseableRDFWriter>();
        for (Output output : outputs) {
            CloseableRDFWriter writer = createOutputWriter(output.getFileLocation(), output.getFormat());
            writers.add(writer);
            writeNamespaceDeclarations(writer, nsPrefixes);
        }
        return writers;
    }

    private static CloseableRDFWriter createOutputWriter(File file, EnumOutputFormat outputFormat) throws IOException {
        OutputStream outputStream = new FileOutputStream(file);
        Writer outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
        switch (outputFormat) {
        case RDF_XML:
            return new IncrementalRdfXmlWriter(outputWriter);
        case N3:
        default:
            return new IncrementalN3Writer(outputWriter);
        }
    }

    private static void writeNamespaceDeclarations(CloseableRDFWriter writer, Map<String, String> nsPrefixes) throws IOException {
        for (Map.Entry<String, String> entry : nsPrefixes.entrySet()) {
            writer.addNamespace(entry.getKey(), entry.getValue());
        }
    }

    private static Iterator<Triple> crQuadsAsTriples(Iterator<CRQuad> crQuads) {
        return ConvertingIterator.decorate(crQuads, new GenericConverter<CRQuad, Triple>() {
            @Override
            public Triple convert(CRQuad object) {
                return object.getQuad().getTriple();
            }
        });
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

    private static void writeCanonicalURIs(HashSet<String> resolvedCanonicalURIs, File outputFile) throws IOException {
        if (outputFile == null) {
            return;
        }
        if (!outputFile.exists() || outputFile.canWrite()) {
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8"));
            try {
                for (String uri : resolvedCanonicalURIs) {
                    writer.println(uri);
                }
            } finally {
                writer.close();
            }
        } else {
            LOG.error("Cannot write canonical URIs to '{}'", outputFile.getPath());
            // Intentionally do not throw an exception
        }
    }

    private void writeSameAsLinks(final URIMappingIterable uriMapping, List<Output> outputs, Map<String, String> nsPrefixes)
            throws IOException {
        
        List<CloseableRDFWriter> writers = new LinkedList<CloseableRDFWriter>();
        try {
            // Create output writers
            for (Output output : outputs) {
                if (output.getSameAsFileLocation() == null) {
                    continue;
                }
                CloseableRDFWriter writer = createOutputWriter(output.getSameAsFileLocation(), output.getFormat());
                writers.add(writer);
                writer.addNamespace("owl", OWL.getURI());
                writeNamespaceDeclarations(writer, nsPrefixes);
            }
            if (writers.isEmpty()) {
                return;
            }

            GenericConverter<String, Triple> uriToTripleConverter = new GenericConverter<String, Triple>() {
                private final Node sameAs = Node.createURI(OWL.sameAs);

                @Override
                public Triple convert(String uri) {
                    String canonicalUri = uriMapping.getCanonicalURI(uri);
                    return new Triple(
                            Node.createURI(uri),
                            sameAs,
                            Node.createURI(canonicalUri));
                }
            };
            
            for (CloseableRDFWriter writer : writers) {
                Iterator<String> uriIterator = uriMapping.iterator();
                Iterator<Triple> sameAsTripleIterator = ConvertingIterator.decorate(uriIterator, uriToTripleConverter);
                writer.write(sameAsTripleIterator);
            }
        } finally {
            for (CloseableRDFWriter writer : writers) {
                writer.close();
            }
        }
    }
}
