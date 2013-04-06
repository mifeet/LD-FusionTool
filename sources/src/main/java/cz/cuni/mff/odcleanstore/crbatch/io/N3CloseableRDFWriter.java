/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.IOException;
import java.io.OutputStream;

import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.n3.N3WriterFactory;

/**
 * Implementation of {@link CloseableRDFWriter} for N3 output format.
 * @author Jan Michelfeit
 */
public class N3CloseableRDFWriter extends SesameCloseableRDFWriterBase {
    private static final RDFWriterFactory WRITER_FACTORY = new N3WriterFactory();
    
    /**
     * @param outputStream writer to which result is written
     * @throws IOException  I/O error
     */
    public N3CloseableRDFWriter(OutputStream outputStream) throws IOException {
        super(outputStream, WRITER_FACTORY);
    }
}
