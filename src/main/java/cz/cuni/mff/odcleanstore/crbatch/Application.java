package cz.cuni.mff.odcleanstore.crbatch;

import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;

import cz.cuni.mff.odcleanstore.configuration.ConflictResolutionConfig;
import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;
import cz.cuni.mff.odcleanstore.conflictresolution.CRQuad;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolverFactory;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
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
        config.setAggregationSpec(new AggregationSpec());
        // config.getAggregationSpec().setDefaultAggregation(EnumAggregationType.BEST);
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
                
                Collection<Quad> quads = quadLoader.getQuadsForURI(canonicalURI);
                Collection<CRQuad> resolvedQuads = conflictResolver.resolveConflicts(quads);
                
                LOG.info("Resolved {} quads for URI <{}> resulting in {} quads", 
                        new Object[] { quads.size(), canonicalURI, resolvedQuads.size() });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.debug("----------------------------");
        LOG.debug("CR-batch executed in {} ms", System.currentTimeMillis() - startTime);
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
