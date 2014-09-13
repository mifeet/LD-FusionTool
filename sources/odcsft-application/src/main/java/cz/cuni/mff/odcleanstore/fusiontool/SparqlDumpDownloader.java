package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.fusiontool.config.*;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryFactory;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesRepositoryLoader;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSourceImpl;
import cz.cuni.mff.odcleanstore.fusiontool.util.LDFusionToolUtils;
import org.openrdf.model.Statement;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

public class SparqlDumpDownloader {
    private static final Logger LOG = LoggerFactory.getLogger(SparqlDumpDownloader.class);

    public static final File OUTPUT_FILE = new File("C:\\file.n3");
    public static final RDFFormat FILE_SERIALIZATION = RDFFormat.NTRIPLES;
    public static final String SPARQL_ENDPOINT = "http://example.com/sparql";
    public static final String NAMED_GRAPH_RESTRICTION = "FILTER(?g = <http://graph>)";
    public static final String NAMED_GRAPH_RESTRICTION_VAR = "g";
    public static final int SPARQL_RESULT_MAX_ROWS = 9_000;
    public static final int SPARQL_MIN_QUERY_INTERVAL = 2_000;
    public static final int ERROR_RETRY_INTERVAL = 20_000;
    public static final int MAX_RETRY_ATTEMPTS = 1_000;
    public static final int INITIAL_OFFSET = 0;

    public static void main(String[] args) throws Exception {
        boolean useGZip = OUTPUT_FILE.getName().endsWith(".gz");
        Writer tempOutputWriter = createFileWriter(OUTPUT_FILE, useGZip);
        try {
            DataSourceConfigImpl dataSourceConfig = createDataSourceConfig();
            DataSource dataSource = DataSourceImpl.fromConfig(
                    dataSourceConfig,
                    Collections.<String, String>emptyMap(),
                    new RepositoryFactory(LDFTConfigConstants.DEFAULT_FILE_PARSER_CONFIG));
            RDFWriter tempRdfWriter = Rio.createWriter(FILE_SERIALIZATION, tempOutputWriter);
            tempRdfWriter.startRDF();

            RDFWriterWrapper rdfHandler = new RDFWriterWrapper(tempRdfWriter);

            long retryAttempts = 0;
            boolean finished = false;
            while (!finished) {
                int offset = (int) rdfHandler.getCounter() + INITIAL_OFFSET;
                try {
                    loadQuads(dataSource, offset, rdfHandler);
                    finished = true;
                } catch (LDFusionToolException e) {
                    if (retryAttempts < MAX_RETRY_ATTEMPTS) {
                        retryAttempts++;
                        LOG.error("Error loading triples: " + e.getMessage(), e);
                        LOG.info(String.format("An error occurred, retry # %d in %d s", retryAttempts, ERROR_RETRY_INTERVAL / 1_000));
                        Thread.sleep(ERROR_RETRY_INTERVAL);
                    } else {
                        throw e;
                    }
                }
            }

            tempRdfWriter.endRDF();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (tempOutputWriter != null) {
                tempOutputWriter.close();
            }
        }
    }

    private static void loadQuads(DataSource dataSource, int initialOffset, RDFWriterWrapper rdfHandler) throws LDFusionToolException {
        AllTriplesRepositoryLoader loader = new AllTriplesRepositoryLoader(dataSource);
        loader.setInitialOffset(initialOffset);
        try {
            loader.loadAllTriples(rdfHandler);
        } finally {
            loader.close();
        }
    }

    private static DataSourceConfigImpl createDataSourceConfig() {
        DataSourceConfigImpl dataSourceConfig = new DataSourceConfigImpl(EnumDataSourceType.SPARQL, "");
        dataSourceConfig.setNamedGraphRestriction(new SparqlRestrictionImpl(NAMED_GRAPH_RESTRICTION, NAMED_GRAPH_RESTRICTION_VAR));
        dataSourceConfig.getParams().put(ConfigParameters.DATA_SOURCE_SPARQL_ENDPOINT, SPARQL_ENDPOINT);
        dataSourceConfig.getParams().put(ConfigParameters.DATA_SOURCE_SPARQL_MIN_QUERY_INTERVAL, Integer.toString(SPARQL_MIN_QUERY_INTERVAL));
        dataSourceConfig.getParams().put(ConfigParameters.DATA_SOURCE_SPARQL_RESULT_MAX_ROWS, Integer.toString(SPARQL_RESULT_MAX_ROWS));
        return dataSourceConfig;
    }

    private static Writer createFileWriter(File file, boolean useGzip) throws IOException {
        OutputStream outputStream = new FileOutputStream(file);
        if (useGzip) {
            outputStream = new GZIPOutputStream(outputStream, 2048) {
                {
                    this.def.setLevel(Deflater.BEST_SPEED);
                }
            };
        }
        return new BufferedWriter(new OutputStreamWriter(outputStream, Charset.defaultCharset()));
    }

    private static class RDFWriterWrapper extends RDFHandlerBase implements RDFHandler {
        private final RDFWriter writer;
        private long counter = 0;
        private long startTime;

        public RDFWriterWrapper(RDFWriter writer) {
            this.writer = writer;
            this.startTime = System.currentTimeMillis();
        }

        public long getCounter() {
            return counter;
        }

        @Override
        public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
            this.writer.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(Statement st) throws RDFHandlerException {
            this.writer.handleStatement(st);
            counter++;
            if (counter % 100_000 == 0) {
                String time = LDFusionToolUtils.formatProfilingTime(System.currentTimeMillis() - startTime);
                LOG.info(String.format("Stored %,d quads in %s (last %s)\n", counter, time, st));
            }
        }
    }
}
