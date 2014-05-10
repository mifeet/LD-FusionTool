/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.Output;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.EnumSerializationFormat;
import cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryFactory;
import cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryRDFInserter;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import org.openrdf.model.URI;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
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
     * @throws ODCSFusionToolException invalid output configuration
     */
    public CloseableRDFWriter createRDFWriter(Output output) throws IOException, ODCSFusionToolException {
        String name = output.toString();
        URI metadataContext = output.getMetadataContext();
        URI dataContext = output.getDataContext();
        if (dataContext != null) {
            metadataContext = null; // data and metadata context are exclude each other
        }

        switch (output.getType()) {
        case VIRTUOSO:
            return createVirtuosoOutput(
                    name,
                    output.getParams().get(ConfigParameters.OUTPUT_HOST),
                    output.getParams().get(ConfigParameters.OUTPUT_PORT),
                    output.getParams().get(ConfigParameters.OUTPUT_USERNAME),
                    output.getParams().get(ConfigParameters.OUTPUT_PASSWORD),
                    dataContext,
                    metadataContext);
        case SPARQL:
            return createSparqlOutput(
                    name,
                    output.getParams().get(ConfigParameters.OUTPUT_ENDPOINT_URL),
                    output.getParams().get(ConfigParameters.OUTPUT_USERNAME),
                    output.getParams().get(ConfigParameters.OUTPUT_PASSWORD),
                    dataContext,
                    metadataContext);
        case FILE:
            String pathString = output.getParams().get(ConfigParameters.OUTPUT_PATH);
            File fileLocation = pathString != null ? new File(pathString) : null;

            String formatString = output.getParams().get(ConfigParameters.OUTPUT_FORMAT);
            EnumSerializationFormat format = EnumSerializationFormat.parseFormat(formatString);

            String splitByMBString = output.getParams().get(ConfigParameters.OUTPUT_SPLIT_BY_MB);
            Long splitByMB = null;
            if (splitByMBString != null) {
                final String errorMessage = "Value of splitByMB for output " + output.getName() + " is not a positive number";
                try {
                    splitByMB = Long.parseLong(splitByMBString);
                } catch (NumberFormatException e) {
                    throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.OUTPUT_PARAM, errorMessage, e);
                }
                if (splitByMB <= 0) {
                    throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.OUTPUT_PARAM, errorMessage);
                }
            }

            return createFileOutput(name, fileLocation, format, splitByMB, dataContext, metadataContext);
        default:
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.OUTPUT_UNSUPPORTED, "Output of type "
                    + output.getType() + " is not supported");
        }
    }

    /**
     * Returns a new {@link CloseableRDFWriter} for serialization to a file.
     * @param name output name
     * @param fileLocation base file path for output files; n-th file will have suffix -n
     * @param format serialization format
     * @param splitByMB approximate maximum size of each output file in megabytes
     *        (the size is approximate, because after the limit is exceeded, some data may be written to close the file properly)
     * @param dataContext URI of named graph where resolved quads will be placed or null for unique graph per quad
     *        (if the serialization format supports named graphs)
     * @param metadataContext URI of named graph where CR metadata will be placed or null for no metadata
     *        (if the serialization format supports named graphs)
     * @return RDF writer
     * @throws IOException I/O error
     * @throws ODCSFusionToolException invalid output configuration
     */
    private CloseableRDFWriter createFileOutput(String name, File fileLocation, EnumSerializationFormat format,
            Long splitByMB, URI dataContext, URI metadataContext)
            throws IOException, ODCSFusionToolException {

        if (fileLocation == null) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.OUTPUT_PARAM,
                    "Name of output file must be specified for output " + name);
        } else if (format == null) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.OUTPUT_PARAM,
                    "Invalid output format specified for output " + name);
        }

        ODCSFusionToolUtils.ensureParentsExists(fileLocation);
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
     *        (if the serialization format supports named graphs)
     * @param metadataContext URI of named graph where CR metadata will be placed or null for no metadata
     *        (if the serialization format supports named graphs)
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
     *        (the size is approximate, because after the limit is exceeded, some data may be written to close the file properly)
     * @param dataContext URI of named graph where resolved quads will be placed or null for unique graph per quad
     *        (if the serialization format supports named graphs)
     * @param metadataContext URI of named graph where CR metadata will be placed or null for no metadata
     *        (if the serialization format supports named graphs)
     * @return RDF writer
     * @throws IOException I/O error
     */
    private CloseableRDFWriter createSplittingFileRDFWriter(EnumSerializationFormat format, File outputFile,
            long splitByMB, URI dataContext, URI metadataContext)
            throws IOException {

        long splitByBytes = splitByMB * ODCSFusionToolUtils.MB_BYTES;
        return new SplittingRDFWriter(format, outputFile, splitByBytes, this, dataContext, metadataContext);
    }
    
    private CloseableRDFWriter createVirtuosoOutput(String name, String host, String port, String username, String password,
            URI dataContext, URI metadataContext) throws IOException {
        try {
            VirtuosoRDFWriter writer = new VirtuosoRDFWriter(name, host, port, username, password);
            LOG.debug("Initialized Virtuoso output {}", name);
            return new SesameCloseableRDFWriterQuad(writer, writer, dataContext, metadataContext);
        } catch (ODCSFusionToolException e) {
            throw new IOException(e);
        }
    }
    
    private CloseableRDFWriter createSparqlOutput(String name, String endpointURL, String username, String password,
            URI dataContext, URI metadataContext) throws IOException {
        try {
            Repository repository = REPOSITORY_FACTORY.createSparqlRepository(
                    name, endpointURL, endpointURL, username, password);
            RepositoryConnection connection  = repository.getConnection();
            RepositoryRDFInserter rdfWriter = new RepositoryRDFInserter(connection);
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
