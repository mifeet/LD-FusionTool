/**
 *
 */
package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.Output;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolApplicationException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.EnumSerializationFormat;
import cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryFactory;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolAppUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolApplicationUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.OutputParamReader;
import org.openrdf.model.URI;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.repository.util.RDFInserter;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.n3.N3WriterFactory;
import org.openrdf.rio.nquads.NQuadsWriterFactory;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriterFactory;
import org.openrdf.rio.trig.TriGWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Factory class for {@link CloseableRDFWriter} instances.
 * @author Jan Michelfeit
 */
public class CloseableRDFWriterFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CloseableRDFWriterFactory.class);

    // here we don't mind using default parser config instead of one from configuration because there is no parsing
    private static final RepositoryFactory REPOSITORY_FACTORY = new RepositoryFactory(new ParserConfig());

    /**
     * Creates a new {@link CloseableRDFWriter} according to settings given in output configuration.
     * @param output output configuration
     * @return RDF writer
     * @throws IOException I/O error
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException invalid output configuration
     */
    public CloseableRDFWriter createRDFWriter(Output output) throws IOException, ODCSFusionToolException {
        URI metadataContext = output.getMetadataContext();
        URI dataContext = output.getDataContext();
        if (dataContext != null) {
            metadataContext = null; // data and metadata context are exclude each other
        }
        String name = output.toString();
        OutputParamReader paramReader = new OutputParamReader(output);

        switch (output.getType()) {
            case VIRTUOSO:
                return createVirtuosoOutput(paramReader, dataContext, metadataContext, name);
            case SPARQL:
                return createSparqlOutput(paramReader, dataContext, metadataContext, name);
            case FILE:
                return createFileOutput(paramReader, dataContext, metadataContext, name);
            default:
                throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.OUTPUT_UNSUPPORTED, "Output of type "
                        + output.getType() + " is not supported");
        }
    }

    /**
     * Returns a new {@link CloseableRDFWriter} for serialization to a file.
     * @param paramReader
     * @param dataContext URI of named graph where resolved quads will be placed or null for unique graph per quad
     * (if the serialization format supports named graphs)
     * @param metadataContext URI of named graph where CR metadata will be placed or null for no metadata
     * (if the serialization format supports named graphs)
     * @param name output name
     * @return RDF writer
     * @throws IOException I/O error
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException invalid output configuration
     */
    private CloseableRDFWriter createFileOutput(OutputParamReader paramReader, URI dataContext, URI metadataContext, String name)
            throws IOException, ODCSFusionToolException {

        String pathString = paramReader.getRequiredStringValue(ConfigParameters.OUTPUT_PATH);
        File fileLocation = pathString != null ? new File(pathString) : null;
        String formatString = paramReader.getRequiredStringValue(ConfigParameters.OUTPUT_FORMAT);
        EnumSerializationFormat format = EnumSerializationFormat.parseFormat(formatString);
        Long splitByMB = paramReader.getLongValue(ConfigParameters.OUTPUT_SPLIT_BY_MB);
        if (splitByMB != null && splitByMB <= 0) {
            final String errorMessage = "Value of splitByMB for output " + name + " is not a positive number";
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.OUTPUT_PARAM, errorMessage);

        }

        ODCSFusionToolApplicationUtils.ensureParentsExists(fileLocation);
        if (splitByMB == null) {
            return createFileRDFWriter(format, new FileOutputStream(fileLocation), dataContext, metadataContext);
        } else {
            return createSplittingFileRDFWriter(format, fileLocation, splitByMB, dataContext, metadataContext);
        }
    }

    /**
     * Creates a new {@link CloseableRDFWriter} serializing RDF to a file of the given format.
     * @param format serialization format
     * @param outputStream stream to write
     * @param dataContext URI of named graph where resolved quads will be placed or null for unique graph per quad
     * (if the serialization format supports named graphs)
     * @param metadataContext URI of named graph where CR metadata will be placed or null for no metadata
     * (if the serialization format supports named graphs)
     * @return RDF writer
     * @throws IOException I/O error
     */
    public CloseableRDFWriter createFileRDFWriter(EnumSerializationFormat format, OutputStream outputStream,
            URI dataContext, URI metadataContext)
            throws IOException {

        Writer outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
        switch (format) {
            case RDF_XML:
                RDFHandler rdfXmlWriter = new RDFXMLPrettyWriterFactory().getWriter(outputWriter);
                LOG.debug("Created a RDF/XML file output");
                // RDFHandler rdfXmlWriter = new RDFXMLWriterFactory().getWriter(outputWriter);
                return new SesameCloseableRDFWriterTriple(rdfXmlWriter, outputWriter);
            case N3:
                RDFHandler n3Writer = new N3WriterFactory().getWriter(outputWriter);
                LOG.debug("Created an N3 file output");
                return new SesameCloseableRDFWriterTriple(n3Writer, outputWriter);
            case TRIG:
                RDFHandler trigHandler = new TriGWriterFactory().getWriter(outputWriter);
                LOG.debug("Created a TriG file output");
                return new SesameCloseableRDFWriterQuad(trigHandler, outputWriter, dataContext, metadataContext);
            case NQUADS:
                RDFHandler nquadsHandler = new NQuadsWriterFactory().getWriter(outputWriter);
                LOG.debug("Created a N-Quads file output");
                return new SesameCloseableRDFWriterQuad(nquadsHandler, outputWriter, dataContext, metadataContext);
            case HTML:
                LOG.debug("Created a HTML file output");
                return new CloseableHtmlWriter(outputWriter);
            default:
                throw new IllegalArgumentException("Unknown output format " + format);
        }
    }

    /**
     * Returns a new {@link CloseableRDFWriter} which splits output across several files with approximate
     * maximum size given in splitByBytes.
     * @param format serialization format
     * @param outputFile base file path for output files; n-th file will have suffix -n
     * @param splitByMB approximate maximum size of each output file in megabytes
     * (the size is approximate, because after the limit is exceeded, some data may be written to close the file properly)
     * @param dataContext URI of named graph where resolved quads will be placed or null for unique graph per quad
     * (if the serialization format supports named graphs)
     * @param metadataContext URI of named graph where CR metadata will be placed or null for no metadata
     * (if the serialization format supports named graphs)
     * @return RDF writer
     * @throws IOException I/O error
     */
    private CloseableRDFWriter createSplittingFileRDFWriter(EnumSerializationFormat format, File outputFile,
            long splitByMB, URI dataContext, URI metadataContext)
            throws IOException {

        long splitByBytes = splitByMB * ODCSFusionToolAppUtils.MB_BYTES;
        return new SplittingRDFWriter(format, outputFile, splitByBytes, this, dataContext, metadataContext);
    }

    private CloseableRDFWriter createVirtuosoOutput(OutputParamReader paramReader, URI dataContext, URI metadataContext, String name)
            throws IOException, ODCSFusionToolException {
        String host = paramReader.getRequiredStringValue(ConfigParameters.OUTPUT_HOST);
        String port = paramReader.getRequiredStringValue(ConfigParameters.OUTPUT_PORT);
        String username = paramReader.getStringValue(ConfigParameters.OUTPUT_USERNAME);
        String password = paramReader.getStringValue(ConfigParameters.OUTPUT_PASSWORD);
        try {
            VirtuosoRDFWriter writer = new VirtuosoRDFWriter(name, host, port, username, password);
            LOG.debug("Initialized Virtuoso output {}", name);
            return new SesameCloseableRDFWriterQuad(writer, writer, dataContext, metadataContext);
        } catch (ODCSFusionToolException e) {
            throw new IOException(e);
        }
    }

    private CloseableRDFWriter createSparqlOutput(OutputParamReader paramReader, URI dataContext, URI metadataContext, String name)
            throws IOException, ODCSFusionToolException {
        String endpointURL = paramReader.getRequiredStringValue(ConfigParameters.OUTPUT_ENDPOINT_URL);
        String username = paramReader.getStringValue(ConfigParameters.OUTPUT_USERNAME);
        String password = paramReader.getStringValue(ConfigParameters.OUTPUT_PASSWORD);
        try {
            SPARQLRepository repository = new SPARQLRepository(endpointURL, endpointURL);
            if (username != null || password != null) {
                repository.setUsernameAndPassword(username, password);
            }
            try {
                repository.initialize();
            } catch (RepositoryException e) {
                throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_SPARQL, "Error when initializing repository for " + name, e);
            }

            RepositoryConnection connection = repository.getConnection();
            RDFInserter rdfWriter = new RDFInserter(connection);
            Closeable connectionCloser = new ConnectionCloser(connection);
            LOG.debug("Initialized SPARQL output {}", name);
            return new SesameCloseableRDFWriterQuad(rdfWriter, connectionCloser, dataContext, metadataContext);
        } catch (ODCSFusionToolException e) {
            throw new IOException(e);
        } catch (RepositoryException e) {
            throw new IOException(e);
        }
    }

    /**
     * Wrapper for closing a repository connection.
     */

    private class ConnectionCloser implements Closeable {
        private RepositoryConnection connection;

        public ConnectionCloser(RepositoryConnection connection) {
            this.connection = connection;
        }

        @Override
        public void close() throws IOException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (RepositoryException e) {
                    throw new IOException(e);
                }
                connection = null;
            }
        }
    }
}
