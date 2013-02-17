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
import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationType;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
import cz.cuni.mff.odcleanstore.crbatch.io.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.crbatch.io.EnumOutputFormat;
import cz.cuni.mff.odcleanstore.crbatch.io.IncrementalN3Writer;
import cz.cuni.mff.odcleanstore.crbatch.io.IncrementalRdfXmlWriter;
import cz.cuni.mff.odcleanstore.crbatch.loaders.LoaderUtils;
import cz.cuni.mff.odcleanstore.crbatch.loaders.NamedGraphLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.QuadLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.SameAsLinkLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.TripleSubjectsLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.TripleSubjectsLoader.SubjectsIterator;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;
import de.fuberlin.wiwiss.ng4j.Quad;

/**
 * The main entry point of the application.
 * @author Jan Michelfeit
 */
public final class Application {
    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    /**
     * @param args
     * 
     */
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        Config config = new Config();
        config.setDatabaseConnectionString("jdbc:virtuoso://localhost:1111/CHARSET=UTF-8");
        config.setDatabasePassword("dba");
        config.setDatabaseUsername("dba");
        config.setOutputFormat(EnumOutputFormat.N3);
        config.setAggregationSpec(new AggregationSpec());
        config.getAggregationSpec().setDefaultAggregation(EnumAggregationType.BEST);
        config.setNamedGraphConstraintPattern(LoaderUtils.preprocessGroupGraphPattern(
                "?" + ConfigConstants.NG_CONSTRAINT_VAR + " <" + ODCS.isLatestUpdate + "> ?x FILTER(?x = 1)"));
        // config.setNamedGraphConstraintPattern(QueryUtils.preprocessGroupGraphPattern(
        // ConfigConstants.NG_CONSTRAINT_PATTERN_VARIABLE + " <" + ODCS.metadataGraph + "> ?x"));
        //

        // TODO: check valid input
        
        
        ConnectionFactory connectionFactory = new ConnectionFactory(config);
        try {
            NamedGraphLoader graphLoader = new NamedGraphLoader(connectionFactory, config.getNamedGraphConstraintPattern());
            NamedGraphMetadataMap namedGraphsMetadata = graphLoader.getNamedGraphs();

            // Load & resolve owl:sameAs links
            SameAsLinkLoader sameAsLoader = new SameAsLinkLoader(connectionFactory, config.getNamedGraphConstraintPattern());
            URIMappingIterable uriMapping = sameAsLoader.getSameAsMappings();
            AlternativeURINavigator alternativeURINavigator = new AlternativeURINavigator(uriMapping);
            
            // Get iterator over subjects of relevant triples
            TripleSubjectsLoader tripleSubjectsLoader = new TripleSubjectsLoader(connectionFactory,
                    config.getNamedGraphConstraintPattern());
            SubjectsIterator subjectsIterator = tripleSubjectsLoader.getTripleSubjectIterator();
            HashSet<String> resolvedCanonicalURIs = new HashSet<String>();
            
            // Initialize CR
            ConflictResolver conflictResolver = createConflictResolver(config, namedGraphsMetadata, uriMapping);
            
            // Initialize output writer
            // TODO: writing could be more (esp. memory) efficient by avoiding models and serializing manually
            Writer outputWriter = createOutputWriter(config);
            CloseableRDFWriter rdfWriter = createRDFWriter(outputWriter, config);
            
            // Load relevant triples (quads) subject by subject so that we can apply CR to them
            QuadLoader quadLoader = new QuadLoader(connectionFactory, config.getNamedGraphConstraintPattern(),
                    alternativeURINavigator);
            
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
                rdfWriter.write(resolvedModel); 
                outputWriter.flush(); // TODO: ?
            }
            
            testBNodes(rdfWriter);
            
            rdfWriter.close();
            outputWriter.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.debug("----------------------------");
        LOG.debug("CR-batch executed in {} ms", System.currentTimeMillis() - startTime);
    }
    
    private static void testBNodes(CloseableRDFWriter rdfWriter) {
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
        
        rdfWriter.write(m1); 
        rdfWriter.write(m2); 
        rdfWriter.write(m3); 
        rdfWriter.write(m4); 
    }

    
    /**
     * @return
     */
    private static Writer createOutputWriter(Config config) throws IOException {
        String fileName = "out." + config.getOutputFormat().getFileExtension(); // TODO
        OutputStream outputStream = new FileOutputStream(new File(fileName));
        Writer outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
        return outputWriter;
    }
    
    private static CloseableRDFWriter createRDFWriter(Writer outputWriter, Config config) {
        switch (config.getOutputFormat()) {
        case RDF_XML:
            IncrementalRdfXmlWriter rdfXmlWriter = new IncrementalRdfXmlWriter(outputWriter);
            // Settings making writing faster
            rdfXmlWriter.setProperty("allowBadURIs", "true");
            rdfXmlWriter.setProperty("relativeURIs", "");
            //rdfXmlWriter.setProperty("tab", "0");
            //return rdfXmlWriter;
            // TODO: rdfXmlWriter doesn't work properly yet
        case N3:
        default:
            return new IncrementalN3Writer(outputWriter);
        }
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
