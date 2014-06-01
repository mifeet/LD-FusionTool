package cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort;

import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigConstants;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RFDHandler which applies URI mapping to processed statements, buffers them up to the size of {@code maxMemoryTriples},
 * and every time the buffer is full or there are no more statements to process sorts and writes the buffer to the
 * underlying {@code outputWriter}.
 * The sorting is in place in order to create sorted runs and facilitate external sort afterwards.
 */
public class ExternalSortingInputLoaderPreprocessor implements RDFHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ExternalSortingInputLoaderPreprocessor.class);

    private final UriMapping uriMapping;
    private final RDFHandler rdfHandler;
    private final ValueFactory valueFactory;
    private final boolean outputMappedSubjectsOnly;
    private final AlternativeUriNavigator alternativeUriNavigator;
    private URI defaultContext = null;
    private long statementCounter = 0;

    public ExternalSortingInputLoaderPreprocessor(
            RDFHandler rdfHandler,
            UriMappingIterable uriMapping,
            ValueFactory valueFactory,
            boolean outputMappedSubjectsOnly) {
        this.uriMapping = uriMapping;
        this.rdfHandler = rdfHandler;
        this.valueFactory = valueFactory;
        this.outputMappedSubjectsOnly = outputMappedSubjectsOnly;
        this.alternativeUriNavigator = outputMappedSubjectsOnly ? new AlternativeUriNavigator(uriMapping) : null;
    }

    public void setDefaultContext(URI defaultContext) {
        this.defaultContext = defaultContext;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        LOG.info("Writing input quads to temporary location");
        // do not delegate to rdfHandler
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        if (outputMappedSubjectsOnly && alternativeUriNavigator != null) {
            Resource subject = statement.getSubject();
            if (!(subject instanceof URI) || !alternativeUriNavigator.hasAlternativeUris(subject.toString())) {
                return; // skip statement whose subject has no alternative URIs
            }
        }

        Statement mappedStatement = applyUriMapping(statement);
        rdfHandler.handleStatement(mappedStatement);

        statementCounter++;
        if (statementCounter % ConfigConstants.LOG_LOOP_SIZE == 0) {
            LOG.debug("... written {} quads to temporary location", statementCounter);
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        // do not delegate to rdfHandler
        LOG.debug("... finished writing {} quads to temporary location", statementCounter);
    }

    /**
     * @see cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictClustersCollection.ConflictClusterIterator
     */
    private Statement applyUriMapping(Statement statement) {
        Resource subject = statement.getSubject();
        Resource subjectMapping = (Resource) mapUriNode(subject, uriMapping);
        URI predicate = statement.getPredicate();
        URI predicateMapping = (URI) mapUriNode(predicate, uriMapping);
        Value object = statement.getObject();
        Value objectMapping = mapUriNode(object, uriMapping);
        Resource context = statement.getContext();
        Resource usedContext = context == null ? defaultContext : context;

        // Intentionally !=
        if (subject != subjectMapping || predicate != predicateMapping || object != objectMapping || context != usedContext) {
            return valueFactory.createStatement(subjectMapping, predicateMapping, objectMapping, usedContext);
        } else {
            return statement;
        }
    }

    /**
     * If mapping contains an URI to map for the passed {@link org.openrdf.model.URI} returns a {@link org.openrdf.model.URI} with the mapped URI, otherwise returns
     * <code>value</code>.
     * @param value a {@link org.openrdf.model.Value} to apply mapping to
     * @param uriMapping an URI mapping to apply
     * @return node with applied URI mapping
     */
    private Value mapUriNode(Value value, UriMapping uriMapping) {
        if (value instanceof URI) {
            return uriMapping.mapURI((URI) value);
        }
        return value;
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {
        rdfHandler.handleComment(comment);
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
        rdfHandler.handleNamespace(prefix, uri);
    }
}
