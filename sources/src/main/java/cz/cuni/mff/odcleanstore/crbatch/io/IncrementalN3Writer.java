/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.IOException;
import java.io.Writer;

import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.n3.N3WriterFactory;

/**
 * Implementation of {@link CloseableRDFWriter} for N3 output format.
 * @author Jan Michelfeit
 */
public class IncrementalN3Writer extends SesameCloseableWriterBase {
    private static final RDFWriterFactory WRITER_FACTORY = new N3WriterFactory();
    
    /**
     * @param outputWriter writer to which result is written
     * @throws IOException  I/O error
     */
    public IncrementalN3Writer(Writer outputWriter) throws IOException {
        super(outputWriter, WRITER_FACTORY);
    }
}
