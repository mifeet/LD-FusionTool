package cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort;

import cz.cuni.mff.odcleanstore.fusiontool.config.LDFTConfigConstants;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
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

    private final RDFHandler rdfHandler;
    private final ValueFactory valueFactory;
    private URI defaultContext = null;
    private long statementCounter = 0;

    public ExternalSortingInputLoaderPreprocessor(
            RDFHandler rdfHandler,
            ValueFactory valueFactory) {
        this.rdfHandler = rdfHandler;
        this.valueFactory = valueFactory;
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
        Statement mappedStatement = fillDefaultContext(statement);
        rdfHandler.handleStatement(mappedStatement);

        statementCounter++;
        if (statementCounter % LDFTConfigConstants.LOG_LOOP_SIZE == 0) {
            LOG.debug("... written {} quads to temporary location", statementCounter);
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        // do not delegate to rdfHandler
        LOG.debug("... finished writing {} quads to temporary location", statementCounter);
    }

    private Statement fillDefaultContext(Statement statement) {
        if (statement.getContext() == null) {
            return valueFactory.createStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(), defaultContext);
        } else {
            return statement;
        }
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
