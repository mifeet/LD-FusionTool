package cz.cuni.mff.odcleanstore.crbatch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.simpleframework.xml.core.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import cz.cuni.mff.odcleanstore.configuration.ConflictResolutionConfig;
import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;
import cz.cuni.mff.odcleanstore.conflictresolution.CRQuad;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.crbatch.config.Config;
import cz.cuni.mff.odcleanstore.crbatch.config.ConfigReader;
import cz.cuni.mff.odcleanstore.crbatch.config.Output;
import cz.cuni.mff.odcleanstore.crbatch.config.QueryConfig;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.InvalidInputException;
import cz.cuni.mff.odcleanstore.crbatch.io.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.crbatch.io.IncrementalN3Writer;
import cz.cuni.mff.odcleanstore.crbatch.io.IncrementalRdfXmlWriter;
import cz.cuni.mff.odcleanstore.crbatch.loaders.NamedGraphLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.QuadLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.SameAsLinkLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.TripleSubjectsLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.TripleSubjectsLoader.SubjectsIterator;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;
import de.fuberlin.wiwiss.ng4j.Quad;

/**
 * The main entry point of the application.
 * @author Jan Michelfeit
 */
public final class Application {
    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    private static String getUsage() {
        return "Usage:\n java -jar odcs-cr-batch-<version>.jar <config file>.xml";
    }

    /**
     * Main application entry point.
     * @param args command line arguments
     */
    public static void main(String[] args) {
        if (args == null || args.length < 1) {
            System.err.println(getUsage());
            return;
        }
        File configFile = new File(args[0]);
        if (!configFile.isFile() || !configFile.canRead()) {
            System.err.println("Cannot read the given config file.\n");
            System.err.println(getUsage());
            return;
        }

        Config config = null;
        try {
            config = ConfigReader.parseConfigXml(configFile);
        } catch (InvalidInputException e) {
            System.err.println("Error in config file:");
            System.err.println("  " + e.getMessage());
            if (e.getCause() instanceof PersistenceException) {
                System.err.println("  " + e.getCause().getMessage());
            }
            return;
        }

        // TODO: check valid input (incl. validity of prefixes)
        
        long startTime = System.currentTimeMillis();
        System.out.println("Starting conflict resolution batch, this may take a while... \n");
        
        try {
            executeCRBatch(config);
        } catch (CRBatchException e) {
            System.err.println("Error:");
            System.err.println("  " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("  " + e.getCause().getMessage());
            }
            return;
        } catch (ConflictResolutionException e) {
            System.err.println("Conflict resolution error:");
            System.err.println("  " + e.getMessage());
            return;
        } catch (IOException e) {
            System.err.println("Error when writing results:");
            System.err.println("  " + e.getMessage());
            return;
        }

        System.out.println("----------------------------");
        System.out.printf("CR-batch executed in %.3f s\n",
                (System.currentTimeMillis() - startTime) / (double) ODCSUtils.MILLISECONDS);
    }

    /**
     * Performs the actual CR-batch task.
     * @param config global configuration
     * @throws CRBatchException general batch error
     * @throws IOException I/O error when writing results
     * @throws ConflictResolutionException conflict resolution error
     */
    private static void executeCRBatch(Config config) throws CRBatchException, IOException, ConflictResolutionException {
        ConnectionFactory connectionFactory = new ConnectionFactory(config);

        // Load source named graphs metadata
        NamedGraphLoader graphLoader = new NamedGraphLoader(connectionFactory, (QueryConfig) config);
        NamedGraphMetadataMap namedGraphsMetadata = graphLoader.getNamedGraphs();

        // Load & resolve owl:sameAs links
        SameAsLinkLoader sameAsLoader = new SameAsLinkLoader(connectionFactory, (QueryConfig) config);
        URIMappingIterable uriMapping = sameAsLoader.getSameAsMappings();
        AlternativeURINavigator alternativeURINavigator = new AlternativeURINavigator(uriMapping);

        // Get iterator over subjects of relevant triples
        TripleSubjectsLoader tripleSubjectsLoader = new TripleSubjectsLoader(connectionFactory, (QueryConfig) config);
        SubjectsIterator subjectsIterator = tripleSubjectsLoader.getTripleSubjectIterator();

        // Initialize CR
        ConflictResolver conflictResolver = createConflictResolver(config, namedGraphsMetadata, uriMapping);

        // Initialize output writer
        // TODO: writing could be more (esp. memory) efficient by avoiding models and serializing manually
        List<CloseableRDFWriter> rdfWriters = createRDFWriters(config.getOutputs());

        try {

            // Load & process relevant triples (quads) subject by subject so that we can apply CR to them
            QuadLoader quadLoader = new QuadLoader(connectionFactory, config, alternativeURINavigator);
            HashSet<String> resolvedCanonicalURIs = new HashSet<String>();
            while (subjectsIterator.hasNext()) {
                Node nextSubject = subjectsIterator.next();
                String uri;
                if (nextSubject.isURI()) {
                    uri = nextSubject.getURI();
                } else if (nextSubject.isBlank()) {
                    uri = ODCSUtils.getVirtuosoURIForBlankNode(nextSubject);
                } else {
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
                // TODO: remove
                LOG.info("Resolved {} quads for URI <{}> resulting in {} quads",
                        new Object[] { quads.size(), canonicalURI, resolvedQuads.size() });

                // Write result to output
                Model resolvedModel = crQuadsAsModel(resolvedQuads);
                for (CloseableRDFWriter writer : rdfWriters) {
                    writer.write(resolvedModel);
                }

                // Somehow helps Virtuoso release connections.
                // Without call to Thread.sleep(), application may fail with
                // "No buffer space available (maximum connections reached?)"
                // exception for too many named graphs.
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            // testBNodes(rdfWriters);
        } finally {
            for (CloseableRDFWriter writer : rdfWriters) {
                writer.close();
            }
        }
    }

//    private static void testBNodes(List<CloseableRDFWriter> rdfWriters) {
//        Node b1 = Node.createAnon(new AnonId("anon1"));
//        Node b2 = Node.createAnon(new AnonId("anon2"));
//        Node r = Node.createURI("http://example.com");
//        Node r2 = Node.createURI("http://example2.com");
//        Model m1 = ModelFactory.createDefaultModel();
//        m1.add(m1.asStatement(new Triple(r, r, b1)));
//        m1.add(m1.asStatement(new Triple(r, r2, b2)));
//        Model m2 = ModelFactory.createDefaultModel();
//        m2.add(m2.asStatement(new Triple(r, r, r)));
//        Model m3 = ModelFactory.createDefaultModel();
//        m3.add(m3.asStatement(new Triple(r, r, b1)));
//        Model m4 = ModelFactory.createDefaultModel();
//        m4.add(m4.asStatement(new Triple(r, r2, b2)));
//
//        for (CloseableRDFWriter writer : rdfWriters) {
//            writer.write(m1);
//            writer.write(m2);
//            writer.write(m3);
//            writer.write(m4);
//        }
//    }

    private static List<CloseableRDFWriter> createRDFWriters(List<Output> outputs) throws IOException {
        List<CloseableRDFWriter> writers = new LinkedList<CloseableRDFWriter>();
        for (Output output : outputs) {
            Writer writer = createOutputWriter(output.getFileLocation());
            switch (output.getFormat()) {
            case RDF_XML:
                IncrementalRdfXmlWriter rdfXmlWriter = new IncrementalRdfXmlWriter(writer);
                // Settings making writing faster
                rdfXmlWriter.setProperty("allowBadURIs", "true");
                rdfXmlWriter.setProperty("relativeURIs", "");
                // rdfXmlWriter.setProperty("tab", "0");
                // TODO: rdfXmlWriter doesn't work properly yet
                writers.add(rdfXmlWriter);
                break;
            case N3:
            default:
                IncrementalN3Writer n3writer = new IncrementalN3Writer(writer);
                writers.add(n3writer);
                break;
            }
        }
        return writers;
    }

    private static Writer createOutputWriter(File file) throws IOException {
        OutputStream outputStream = new FileOutputStream(file);
        Writer outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
        return outputWriter;
    }

    private static Model crQuadsAsModel(Collection<CRQuad> crQuads) {
        Model model = ModelFactory.createDefaultModel();
        for (CRQuad crQuad : crQuads) {
            model.add(model.asStatement(crQuad.getQuad().getTriple()));
        }
        return model;
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

    /** Disable constructor. */
    private Application() {
    }
}
