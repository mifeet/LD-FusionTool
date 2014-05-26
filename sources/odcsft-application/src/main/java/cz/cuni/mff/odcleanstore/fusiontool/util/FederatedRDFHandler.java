package cz.cuni.mff.odcleanstore.fusiontool.util;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * Wrapper for multiple {@link org.openrdf.rio.RDFHandler}s delegating calls to each of them.
 */
public class FederatedRDFHandler implements RDFHandler {

    private final RDFHandler[] rdfHandlers;

    public FederatedRDFHandler(RDFHandler... rdfHandlers) {
        ODCSFusionToolUtils.checkNotNull(rdfHandlers);
        this.rdfHandlers = rdfHandlers;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        for (RDFHandler rdfHandler : rdfHandlers) {
            rdfHandler.startRDF();
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        for (RDFHandler rdfHandler : rdfHandlers) {
            rdfHandler.endRDF();
        }
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
        for (RDFHandler rdfHandler : rdfHandlers) {
            rdfHandler.handleNamespace(prefix, uri);
        }
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        for (RDFHandler rdfHandler : rdfHandlers) {
            rdfHandler.handleStatement(st);
        }
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {
        for (RDFHandler rdfHandler : rdfHandlers) {
            rdfHandler.handleComment(comment);
        }
    }
}
