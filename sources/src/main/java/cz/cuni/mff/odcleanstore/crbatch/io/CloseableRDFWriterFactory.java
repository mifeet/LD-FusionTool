/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.openrdf.model.URI;

/**
 * Factory class for {@link CloseableRDFWriter} instances.
 * @author Jan Michelfeit
 */
public class CloseableRDFWriterFactory {
    /**
     * Creates a new {@link CloseableRDFWriter} serializing RDF to the given format.
     * @param format serialization format
     * @param metadataContext URI of named graph where CR metadata will be placed 
     *        (if the serialization format supports named graphs)
     * @param outputStream stream to write
     * @return RDF writer
     * @throws IOException I/O error
     */
    public CloseableRDFWriter createRDFWriter(EnumSerializationFormat format, URI metadataContext, OutputStream outputStream)
            throws IOException {
        switch (format) {
        case RDF_XML:
            return new RdfXmlCloseableRDFWriter(outputStream);
        case N3:
            return new N3CloseableRDFWriter(outputStream);
        case TRIG:
            return new TriGCloseableRDFWriter(outputStream, metadataContext);
        default:
            throw new IllegalArgumentException("Unknown output format " + format);
        }
    }

    /**
     * Returns a new {@link CloseableRDFWriter} which splits output across several files with approximate
     * maximum size given in splitByBytes.
     * @param format serialization format
     * @param metadataContext URI of named graph where CR metadata will be placed
     *        (if the serialization format supports named graphs)
     * @param outputFile base file path for output files; n-th file will have suffix -n
     * @param splitByBytes approximate maximum size of each output file in bytes
     *        (the size is approximate, because after the limit is exceeded, some data may be written to close the file properly)
     * @return RDF writer
     * @throws IOException I/O error
     */
    public CloseableRDFWriter createSplittingRDFWriter(EnumSerializationFormat format, URI metadataContext,
            File outputFile, long splitByBytes) throws IOException {
        return new SplittingRDFWriter(format, metadataContext, outputFile, splitByBytes, this);
    }
}
