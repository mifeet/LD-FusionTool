/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.IOException;
import java.io.Writer;

import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriterFactory;

/**
 * Implementation of {@link CloseableRDFWriter} for RDF/XML output format.
 * @author Jan Michelfeit
 */
public class IncrementalRdfXmlWriter extends SesameCloseableWriterBase {
    private static final RDFWriterFactory WRITER_FACTORY = new RDFXMLPrettyWriterFactory();
    //private static final RDFWriterFactory WRITER_FACTORY = new RDFXMLWriterFactory();
    
    
    /**
     * @param outputWriter writer to which result is written
     * @throws IOException  I/O error
     */
    public IncrementalRdfXmlWriter(Writer outputWriter) throws IOException {
        super(outputWriter, WRITER_FACTORY);
    }
}
