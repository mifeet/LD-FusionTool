/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.io.IOException;
import java.util.Iterator;

import org.openrdf.model.Statement;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.util.Closeable;

/**
 * RDF writer to which multiple triples can be written and which should be closed once it is no longer needed.
 * The writer must preserve IDs for blank nodes between write() calls.
 * @author Jan Michelfeit
 */
public interface CloseableRDFWriter extends Closeable<IOException> {
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
     * @param resolvedStatements {@link CRQuad CRQUads} to write
     * @throws IOException I/O error
     */
    void writeCRQuads(Iterator<ResolvedStatement> resolvedStatements) throws IOException;
    
    /**
     * Write a single CRQuad.
     * @param resolvedStatement CRQuad to write
     * @throws IOException I/O error
     */
    void write(ResolvedStatement resolvedStatement) throws IOException;
    
    /**
     * Add a namespace prefix.
     * Note that this call may have no effect for some writers. 
     * @param prefix namespace prefix
     * @param uri namespace uri
     * @throws IOException I/O error
     */
    void addNamespace(String prefix, String uri) throws IOException;
}
