/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import com.hp.hpl.jena.rdf.model.Model;

import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;

/**
 * RDF writer to which multiple {@link Model}s can be written and which should be closed once it is no longer needed.
 * @author Jan Michelfeit
 */
public interface CloseableRDFWriter extends Closeable {
    /**
     * Write RDF data from the given model.
     * @param model RDF model
     */
    void write(Model model);
}
