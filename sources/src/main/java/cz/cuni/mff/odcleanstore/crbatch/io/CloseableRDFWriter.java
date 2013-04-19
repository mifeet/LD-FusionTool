/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.IOException;
import java.util.Iterator;

import org.openrdf.model.Statement;

import cz.cuni.mff.odcleanstore.conflictresolution.CRQuad;
import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;

/**
 * RDF writer to which multiple triples can be written and which should be closed once it is no longer needed.
 * The writer must preserve IDs for blank nodes between write() calls.
 * @author Jan Michelfeit
 */
public interface CloseableRDFWriter extends Closeable {
    /**
     * Write RDF data.
     * @param quads statements to write
     * @throws IOException I/O error
     */
    void writeQuads(Iterator<Statement> quads) throws IOException;
    
    /**
     * Write a single quad.
     * @param quad statement to write
     * @throws IOException I/O error
     */
    void write(Statement quad) throws IOException;
    
    /**
     * Write quads resolved by Conflict Resolution.
     * @param resolvedQuads {@link CRQuad CRQUads} to write
     * @throws IOException I/O error
     */
    void writeCRQuads(Iterator<CRQuad> resolvedQuads) throws IOException;
    
    /**
     * Write a single CRQuad.
     * @param crQuad CRQuad to write
     * @throws IOException I/O error
     */
    void write(CRQuad crQuad) throws IOException;
    
    /**
     * Add a namespace prefix.
     * Note that this call may have no effect for some writers. 
     * @param prefix namespace prefix
     * @param uri namespace uri
     * @throws IOException I/O error
     */
    void addNamespace(String prefix, String uri) throws IOException;
}
