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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import cz.cuni.mff.odcleanstore.configuration.ConflictResolutionConfig;
import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;
import cz.cuni.mff.odcleanstore.conflictresolution.CRQuad;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
import cz.cuni.mff.odcleanstore.crbatch.config.Config;
import cz.cuni.mff.odcleanstore.crbatch.config.ConfigReader;
import cz.cuni.mff.odcleanstore.crbatch.config.Output;
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

    /**
     * @param args command line arguments
     */
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        File configFile = new File("./data/sample-config.xml");
        Config config = null;
        try {
            config = ConfigReader.parseConfigXml(configFile);
        } catch (InvalidInputException e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO: check valid input

        ConnectionFactory connectionFactory = new ConnectionFactory(config);
        try {
            NamedGraphLoader graphLoader = new NamedGraphLoader(connectionFactory, config.getNamedGraphRestrictionPattern(),
                    config.getNamedGraphRestrictionVar());
            NamedGraphMetadataMap namedGraphsMetadata = graphLoader.getNamedGraphs();

            // Load & resolve owl:sameAs links
            SameAsLinkLoader sameAsLoader = new SameAsLinkLoader(connectionFactory, config.getNamedGraphRestrictionPattern(),
                    config.getNamedGraphRestrictionVar());
            URIMappingIterable uriMapping = sameAsLoader.getSameAsMappings();
            AlternativeURINavigator alternativeURINavigator = new AlternativeURINavigator(uriMapping);

            // Get iterator over subjects of relevant triples
            TripleSubjectsLoader tripleSubjectsLoader = new TripleSubjectsLoader(connectionFactory,
                    config.getNamedGraphRestrictionPattern(), config.getNamedGraphRestrictionVar());
            SubjectsIterator subjectsIterator = tripleSubjectsLoader.getTripleSubjectIterator();
            HashSet<String> resolvedCanonicalURIs = new HashSet<String>();

            // Initialize CR
            ConflictResolver conflictResolver = createConflictResolver(config, namedGraphsMetadata, uriMapping);

            // Initialize output writer
            // TODO: writing could be more (esp. memory) efficient by avoiding models and serializing manually
            List<CloseableRDFWriter> rdfWriters = createRDFWriters(config.getOutputs());

            // Load relevant triples (quads) subject by subject so that we can apply CR to them
            QuadLoader quadLoader = new QuadLoader(connectionFactory, config.getNamedGraphRestrictionPattern(),
                    config.getNamedGraphRestrictionVar(), alternativeURINavigator);

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
            }

            testBNodes(rdfWriters);

            for (CloseableRDFWriter writer : rdfWriters) {
                writer.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.debug("----------------------------");
        LOG.debug("CR-batch executed in {} ms", System.currentTimeMillis() - startTime);
    }

    private static void testBNodes(List<CloseableRDFWriter> rdfWriters) {
        Node b1 = Node.createAnon(new AnonId("anon1"));
        Node b2 = Node.createAnon(new AnonId("anon2"));
        Node r = Node.createURI("http://example.com");
        Node r2 = Node.createURI("http://example2.com");
        Model m1 = ModelFactory.createDefaultModel();
        m1.add(m1.asStatement(new Triple(r, r, b1)));
        m1.add(m1.asStatement(new Triple(r, r2, b2)));
        Model m2 = ModelFactory.createDefaultModel();
        m2.add(m2.asStatement(new Triple(r, r, r)));
        Model m3 = ModelFactory.createDefaultModel();
        m3.add(m3.asStatement(new Triple(r, r, b1)));
        Model m4 = ModelFactory.createDefaultModel();
        m4.add(m4.asStatement(new Triple(r, r2, b2)));

        for (CloseableRDFWriter writer : rdfWriters) {
            writer.write(m1);
            writer.write(m2);
            writer.write(m3);
            writer.write(m4);
        }
    }

    private static Writer createOutputWriter(File file) throws IOException {
        OutputStream outputStream = new FileOutputStream(file);
        Writer outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
        return outputWriter;
    }

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

    /**
     * @param resolvedQuads
     * @return
     */
    private static Model crQuadsAsModel(Collection<CRQuad> crQuads) {
        Model model = ModelFactory.createDefaultModel();
        for (CRQuad crQuad : crQuads) {
            model.add(model.asStatement(crQuad.getQuad().getTriple()));
        }
        return model;
    }

    /**
     * @param config
     * @param namedGraphsMetadata
     * @param uriMapping
     * @return
     */
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
