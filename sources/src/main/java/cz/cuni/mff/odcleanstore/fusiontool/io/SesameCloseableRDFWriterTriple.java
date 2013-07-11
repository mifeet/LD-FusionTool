/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.io.Closeable;
import java.io.IOException;

import org.openrdf.rio.RDFHandler;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;

/**
 * Implementation of {@link CloseableRDFWriter} writing to a given Sesame {@link RDFHandler} writing only triples
 * regardless of named graphs.
 * @author Jan Michelfeit
 */
public class SesameCloseableRDFWriterTriple extends SesameCloseableRDFWriterBase {
    /**
     * @param rdfWriter handler or written RDF data
     * @param underlyingResource a resource to be closed as soon as writing is finished
     * @throws IOException I/O error
     */
    public SesameCloseableRDFWriterTriple(RDFHandler rdfWriter, Closeable underlyingResource) throws IOException {
        super(rdfWriter, underlyingResource);
    }

    @Override
    public void write(ResolvedStatement resolvedStatement) throws IOException {
        write(resolvedStatement.getStatement());
    }
}
