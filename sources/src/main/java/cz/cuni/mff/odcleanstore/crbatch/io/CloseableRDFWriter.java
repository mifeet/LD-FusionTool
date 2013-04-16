/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.IOException;
import java.util.Iterator;

import org.openrdf.model.Statement;

import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;

/**
 * RDF writer to which multiple triples can be written and which should be closed once it is no longer needed.
 * The writer must preserve IDs for blank nodes between write() calls.
 * @author Jan Michelfeit
 */
public interface CloseableRDFWriter extends Closeable {
    /**
     * Write RDF data.
     * @param statements statements to write
     * @throws IOException I/O error
     */
    void write(Iterator<Statement> statements) throws IOException;
    
    /**
     * Write a single RDF statement.
     * @param statement statement to write
     * @throws IOException I/O error
     */
    void write(Statement statement) throws IOException;

    /**
     * Add a namespace prefix.
     * Note that this call may have no effect for some writers. 
     * @param prefix namespace prefix
     * @param uri namespace uri
     * @throws IOException I/O error
     */
    void addNamespace(String prefix, String uri) throws IOException;
}
