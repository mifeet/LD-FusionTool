/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.io.CountingOutputStream;
import cz.cuni.mff.odcleanstore.fusiontool.io.EnumSerializationFormat;
import cz.cuni.mff.odcleanstore.fusiontool.io.SplitFileNameGenerator;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolAppUtils;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

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
    private final EnumSerializationFormat outputFormat;
    private final URI metadataContext;
    private final URI dataContext;
    private final SplitFileNameGenerator fileNameGenerator;
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
     * @param dataContext URI of named graph where resolved quads will be placed or null for unique graph per quad
     *        (if the serialization format supports named graphs)
     * @param metadataContext URI of named graph where CR metadata will be placed or null for no metadata
     *        (if the serialization format supports named graphs) 
     */
    public SplittingRDFWriter(EnumSerializationFormat outputFormat, File outputFile, long splitByBytes,
            CloseableRDFWriterFactory writerFactory, URI dataContext, URI metadataContext) {
        this.writerFactory = writerFactory;
        this.outputFormat = outputFormat;
        this.metadataContext = metadataContext;
        this.dataContext = dataContext;
        this.fileNameGenerator = new SplitFileNameGenerator(outputFile);
        this.splitByBytes = splitByBytes;
    }

    private CloseableRDFWriter getRDFWriter() throws IOException {
        if (currentRDFWriter == null) {
            File file = fileNameGenerator.nextFile();
            LOG.info("Creating a new output file: {}", file.getName());
            
            ODCSFusionToolAppUtils.ensureParentsExists(file);
            currentOutputStream = new CountingOutputStream(new FileOutputStream(file));
            currentRDFWriter = writerFactory.createFileRDFWriter(outputFormat, currentOutputStream, dataContext, metadataContext);

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
    public void writeResolvedStatements(Iterable<ResolvedStatement> resolvedQuads) throws IOException {
        for (ResolvedStatement resolvedQuad : resolvedQuads) {
            write(resolvedQuad);
        }
    }
    
    @Override
    public void writeQuads(Iterator<Statement> quads) throws IOException {
        while (quads.hasNext()) {
            write(quads.next());
        } 
    }

    @Override
    public void write(Statement quad) throws IOException {
        getRDFWriter().write(quad);
        checkSizeExceeded();
    }
    
    @Override
    public void write(ResolvedStatement crQuad) throws IOException {
        getRDFWriter().write(crQuad);
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
