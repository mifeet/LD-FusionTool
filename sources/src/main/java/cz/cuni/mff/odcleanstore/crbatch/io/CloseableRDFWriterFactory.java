/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Factory class for {@link CloseableRDFWriter} instances.
 * @author Jan Michelfeit
 */
public class CloseableRDFWriterFactory {
    /**
     * Creates a new {@link CloseableRDFWriter} serializing RDF to the given format.
     * @param format output format
     * @param outputStream stream to write
     * @return RDF writer
     * @throws IOException I/O error
     */
    public CloseableRDFWriter createRDFWriter(EnumOutputFormat format, OutputStream outputStream) throws IOException {
        switch (format) {
        case RDF_XML:
            return new RdfXmlCloseableRDFWriter(outputStream);
        case N3:
            return new N3CloseableRDFWriter(outputStream);
        default:
            throw new IllegalArgumentException("Unknown output format " + format);
        }
    }

    /**
     * @param outputFormat
     * @param countingOutputStream
     * @param splitByBytes
     * @return
     * @throws IOException
     */
    public CloseableRDFWriter createSplittingRDFWriter(EnumOutputFormat format, File outputFile, long splitByBytes)
            throws IOException {

        return new SplittingRDFWriter(format, outputFile, splitByBytes, this);
    }
}
