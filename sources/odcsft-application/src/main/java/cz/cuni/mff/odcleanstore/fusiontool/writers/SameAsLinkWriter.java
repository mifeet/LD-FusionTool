package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumOutputType;
import cz.cuni.mff.odcleanstore.fusiontool.config.Output;
import cz.cuni.mff.odcleanstore.fusiontool.config.OutputImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.UriToSameAsIterator;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SameAsLinkWriter implements UriMappingWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SameAsLinkWriter.class);

    /** An instance of {@link CloseableRDFWriterFactory}. */
    private static final CloseableRDFWriterFactory rdfWriterFactory = new CloseableRDFWriterFactory();
    private final List<Output> outputs;
    private final Map<String, String> namespacePrefixes;

    public SameAsLinkWriter(List<Output> outputs, Map<String, String> namespacePrefixes) {
        this.outputs = outputs;
        this.namespacePrefixes = namespacePrefixes;
    }

    @Override
    public void write(UriMappingIterable uriMapping) throws IOException {
        List<CloseableRDFWriter> writers = null;
        try {
            writers = createOutputWriters();

            for (CloseableRDFWriter writer : writers) {
                final Iterator<String> uriIterator = uriMapping.iterator();
                Iterator<Statement> sameAsTripleIterator = new UriToSameAsIterator(uriIterator, uriMapping, ValueFactoryImpl.getInstance());
                writer.writeQuads(sameAsTripleIterator);
            }

            if (!writers.isEmpty() && LOG.isInfoEnabled()) {
                int linkCounter = 0;
                for (String ignored : uriMapping) {
                    linkCounter++;
                }
                LOG.info("Written {} owl:sameAs links", linkCounter);
            }
        } catch (ODCSFusionToolException e) {
            throw new IOException(e);
        } finally {
            if (writers != null) {
                for (CloseableRDFWriter writer : writers) {
                    writer.close();
                }
            }
        }
    }

    private List<CloseableRDFWriter> createOutputWriters() throws IOException, ODCSFusionToolException {
        List<CloseableRDFWriter> writers = new LinkedList<>();
        for (Output output : outputs) {
            if (output.getType() != EnumOutputType.FILE || output.getParams().get(ConfigParameters.OUTPUT_SAME_AS_FILE) == null) {
                continue;
            }

            OutputImpl sameAsOutput = new OutputImpl(EnumOutputType.FILE, output.toString() + "-sameAs");
            sameAsOutput.getParams().put(ConfigParameters.OUTPUT_PATH, output.getParams().get(ConfigParameters.OUTPUT_SAME_AS_FILE));
            sameAsOutput.getParams().put(ConfigParameters.OUTPUT_FORMAT, output.getParams().get(ConfigParameters.OUTPUT_FORMAT));
            sameAsOutput.getParams().put(ConfigParameters.OUTPUT_SPLIT_BY_MB, output.getParams().get(ConfigParameters.OUTPUT_SPLIT_BY_MB));
            CloseableRDFWriter writer = rdfWriterFactory.createRDFWriter(sameAsOutput);
            writers.add(writer);
            writeNamespaces(namespacePrefixes, writer);
        }
        return writers;
    }

    private void writeNamespaces(Map<String, String> namespacePrefixes, CloseableRDFWriter writer) throws IOException {
        writer.addNamespace("owl", OWL.NAMESPACE);
        for (Map.Entry<String, String> entry : namespacePrefixes.entrySet()) {
            writer.addNamespace(entry.getKey(), entry.getValue());
        }
    }
}
