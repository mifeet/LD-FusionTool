package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigConstants;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfigImpl;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestrictionImpl;
import cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryFactory;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesRepositoryLoader;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSourceImpl;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolAppUtils;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

public class SparqlDumpDownloader {
    public static final File OUTPUT_FILE = new File("C:\\file.n3");
    public static final RDFFormat FILE_SERIALIZATION = RDFFormat.NTRIPLES;
    public static final String SPARQL_ENDPOINT = "http://example.com/sparql";
    public static final String NAMED_GRAPH_RESTRICTION = "FILTER(?g = <http://graph>)";
    public static final String NAMED_GRAPH_RESTRICTION_VAR = "g";
    public static final int SPARQL_RESULT_MAX_ROWS = 9000;
    public static final int SPARQL_MIN_QUERY_INTERVAL = 2000;
    public static final int INITIAL_OFFSET = 0;

    public static void main(String[] args) throws Exception {
        boolean useGZip = OUTPUT_FILE.getName().endsWith(".gz");
        Writer tempOutputWriter = createFileWriter(OUTPUT_FILE, useGZip);
        try {
            DataSourceConfigImpl dataSourceConfig = createDataSourceConfig();
            DataSource dataSource = DataSourceImpl.fromConfig(
                    dataSourceConfig,
                    Collections.<String, String>emptyMap(),
                    new RepositoryFactory(ConfigConstants.DEFAULT_FILE_PARSER_CONFIG));
            AllTriplesRepositoryLoader loader = new AllTriplesRepositoryLoader(dataSource);
            loader.setInitialOffset(INITIAL_OFFSET);

            RDFWriter tempRdfWriter = Rio.createWriter(FILE_SERIALIZATION, tempOutputWriter);
            tempRdfWriter.startRDF();
            try {
                loader.loadAllTriples(new RDFWriterWrapper(tempRdfWriter));
            } finally {
                loader.close();
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

        @Override
        public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
            this.writer.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(Statement st) throws RDFHandlerException {
            this.writer.handleStatement(st);
            counter++;
            if (counter % 100_000 == 0) {
                String time = ODCSFusionToolAppUtils.formatProfilingTime(System.currentTimeMillis() - startTime);
                System.out.printf("Stored %,d quads in %s (last %s)\n", counter, time, st);
            }
        }
    }
}
