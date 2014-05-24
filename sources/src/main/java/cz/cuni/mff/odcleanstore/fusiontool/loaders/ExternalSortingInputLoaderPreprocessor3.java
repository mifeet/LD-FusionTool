package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.conflictresolution.URIMapping;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.GrowingStatementArray;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.util.StatementSizeEstimator;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;

/**
 * RFDHandler which applies URI mapping to processed statements, buffers them up to the size of {@code maxMemoryTriples},
 * and every time the buffer is full or there are no more statements to process sorts and writes the buffer to the
 * underlying {@code outputWriter}.
 * The sorting is in place in order to create sorted runs and facilitate external sort afterwards.
 */
public class ExternalSortingInputLoaderPreprocessor3 extends RDFHandlerBase implements RDFHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ExternalSortingInputLoaderPreprocessor3.class);
    public static final int INITIAL_CAPACITY = 1000;

    private final URIMapping uriMapping;
    private final RDFHandler outputWriter;
    private final ValueFactory valueFactory;
    private final Comparator<Statement> orderComparator;
    private final long memoryLimit;
    private final boolean outputMappedSubjectsOnly;
    private final AlternativeURINavigator alternativeUriNavigator;
    private URI defaultContext = null;
    GrowingStatementArray buffer;
    private long bufferedSize;

    public ExternalSortingInputLoaderPreprocessor3(
            URIMappingIterable uriMapping,
            RDFHandler outputWriter,
            long memoryLimit,
            ValueFactory valueFactory,
            Comparator<Statement> orderComparator,
            boolean outputMappedSubjectsOnly) {
        this.uriMapping = uriMapping;
        this.outputWriter = outputWriter;
        this.valueFactory = valueFactory;
        this.orderComparator = orderComparator;
        this.memoryLimit = memoryLimit;
        this.outputMappedSubjectsOnly = outputMappedSubjectsOnly;
        this.alternativeUriNavigator = outputMappedSubjectsOnly ? new AlternativeURINavigator(uriMapping) : null;
    }

    public void setDefaultContext(URI defaultContext) {
        this.defaultContext = defaultContext;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        buffer = new GrowingStatementArray(INITIAL_CAPACITY);
        bufferedSize = 0;
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        flushBuffer(false);
        buffer = null;
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
        long estimatedSize = StatementSizeEstimator.estimatedSizeOf(mappedStatement);
        if (bufferedSize + estimatedSize > memoryLimit) {
            flushBuffer(true);
        }
        buffer.add(mappedStatement);
        bufferedSize += estimatedSize;
    }

    private void flushBuffer(boolean initializeNew) throws RDFHandlerException {
        // Sort
        Statement[] statements = buffer.getArray();
        int count = buffer.size();
        Arrays.sort(statements, 0, count, orderComparator);

        // Write
        LOG.info("Writing {} quads to temporary location", buffer.size());
        for (int i = 0; i < count; i++) {
            outputWriter.handleStatement(statements[i]);
        }

        // Clear the buffer
        buffer = null;
        bufferedSize = 0;
        if (initializeNew) {
            buffer = new GrowingStatementArray(INITIAL_CAPACITY);
        }
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
    private Value mapUriNode(Value value, URIMapping uriMapping) {
        if (value instanceof URI) {
            return uriMapping.mapURI((URI) value);
        }
        return value;
    }
}
