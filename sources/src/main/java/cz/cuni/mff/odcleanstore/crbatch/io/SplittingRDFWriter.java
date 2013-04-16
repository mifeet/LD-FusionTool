/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.crbatch.util.CRBatchUtils;

/**
 * {@link CloseableRDFWriter} implementation which splits output across several files with the given maximum size.
 * @author Jan Michelfeit
 */
public class SplittingRDFWriter implements CloseableRDFWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SplittingRDFWriter.class);
    
    // CHECKSTYLE:OFF
    private static class NamespaceDeclaration {
        public final String prefix;
        public final String uri;

        public NamespaceDeclaration(String prefix, String uri) {
            this.prefix = prefix;
            this.uri = uri;
        }
    }
    // CHECKSTYLE:ON

    private final CloseableRDFWriterFactory writerFactory;
    private final EnumOutputFormat outputFormat;
    private final SplitFileNameGenerator fileNameGenrator;
    private final long splitByBytes;
    private final ArrayList<NamespaceDeclaration> namespaceDeclarations = new ArrayList<NamespaceDeclaration>();
    private CloseableRDFWriter currentRDFWriter;
    private CountingOutputStream currentOutputStream;

    /**
     * Creates a new RDF writer which splits output across several files with approximate
     * maximum size given in splitByBytes. 
     * @param outputFormat serialization format
     * @param outputFile base file path for output files; n-th file will have suffix -n
     * @param splitByBytes approximate maximum size of each output file in bytes 
     *      (the size is approximate, because after the limit is exceeded, some data may be written to close the file properly)
     * @param writerFactory factory for underlying RDF writers used to do the actual serialization 
     * @throws IOException I/O error
     */
    public SplittingRDFWriter(EnumOutputFormat outputFormat, File outputFile, long splitByBytes,
            CloseableRDFWriterFactory writerFactory)
            throws IOException {
        this.writerFactory = writerFactory;
        this.outputFormat = outputFormat;
        this.fileNameGenrator = new SplitFileNameGenerator(outputFile);
        this.splitByBytes = splitByBytes;
    }

    private CloseableRDFWriter getRDFWriter() throws IOException {
        if (currentRDFWriter == null) {
            File file = fileNameGenrator.nextFile();
            LOG.info("Creating a new output file: {}", file.getName());
            
            CRBatchUtils.ensureParentsExists(file);
            currentOutputStream = new CountingOutputStream(new FileOutputStream(file));
            currentRDFWriter = writerFactory.createRDFWriter(outputFormat, currentOutputStream);

            // Do not forget namespace declarations whose definitions were in the previous files
            for (NamespaceDeclaration ns : namespaceDeclarations) {
                currentRDFWriter.addNamespace(ns.prefix, ns.uri);
            }
        }
        return currentRDFWriter;
    }

    private void checkSizeExceeded() throws IOException {
        if (currentOutputStream.getByteCount() >= splitByBytes) {
            close();
        }
    }

    @Override
    public void addNamespace(String prefix, String uri) throws IOException {
        namespaceDeclarations.add(new NamespaceDeclaration(prefix, uri));
        getRDFWriter().addNamespace(prefix, uri);
        checkSizeExceeded();
    }

    @Override
    public void write(Iterator<Statement> statements) throws IOException {
        while (statements.hasNext()) {
            write(statements.next());
        }
    }

    @Override
    public void write(Statement statement) throws IOException {
        getRDFWriter().write(statement);
        checkSizeExceeded();
    }

    @Override
    public void close() throws IOException {
        try {
            if (currentRDFWriter != null) {
                currentRDFWriter.close();
            }
        } finally {
            currentRDFWriter = null;
            currentOutputStream = null;
        }
    }
}
