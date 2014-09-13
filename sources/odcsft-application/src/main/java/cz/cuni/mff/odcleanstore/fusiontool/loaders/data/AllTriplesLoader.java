package cz.cuni.mff.odcleanstore.fusiontool.loaders.data;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.Closeable;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFHandler;

/**
 * Class that loads all relevant triples from an underlying data source and passes them to the given handler.
 */
public interface AllTriplesLoader extends Closeable<LDFusionToolException> {
    /**
     * Loads all quads (triples) from the underlying data source and handle them with the given handler.
     * Methods startRDF() and endRDF() should be called on the handler at the beginning and end, respectively,
     * and loaded quads should be passed by the handleStatement() method.
     * @param rdfHandler handler for loaded statements
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException
     */
    void loadAllTriples(RDFHandler rdfHandler) throws LDFusionToolException;

    /**
     * Returns the default context to be used for loaded statements if they don't have a context themselves.
     * @return the default context URI
     */
    URI getDefaultContext() throws LDFusionToolException;
}
