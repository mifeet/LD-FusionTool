package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import com.google.common.base.Stopwatch;
import cz.cuni.mff.odcleanstore.fusiontool.config.*;
import cz.cuni.mff.odcleanstore.fusiontool.io.EnumSerializationFormat;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Random;
import java.util.Set;

import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ODCSFTTestUtils.createHttpUri;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ExternalSortingInputLoaderPerformanceTest {
    private static final ValueFactoryImpl VALUE_FACTORY = ValueFactoryImpl.getInstance();

    @Rule
    public TemporaryFolder testDir = new TemporaryFolder();

    @Ignore
    @Test
    public void performanceTest() throws Exception {
        final long maxMemorySize = 2400 * ODCSFusionToolUtils.MB_BYTES;
        final int tripleCount = 1000000;

        // Arrange
        Stopwatch initStopwatch = Stopwatch.createStarted();
        File inputFile = testDir.newFile();
        EnumSerializationFormat format = EnumSerializationFormat.NQUADS;
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(inputFile));
        RDFWriter rdfWriter = Rio.createWriter(format.toSesameFormat(), outputStream);
        rdfWriter.startRDF();

        final int randomRange = tripleCount / 10;
        Random random = new Random(System.nanoTime());
        for (int i = 0; i < tripleCount; i++) {
            Statement statement = VALUE_FACTORY.createStatement(
                    createHttpUri("example.com/resource/s" + random.nextInt(randomRange)),
                    createHttpUri("example.com/resource/p" + random.nextInt(randomRange)),
                    createHttpUri("example.com/resource/i" + random.nextInt(randomRange)),
                    createHttpUri("example.com/graph/g" + i));
            rdfWriter.handleStatement(statement);
        }

        rdfWriter.endRDF();
        outputStream.close();
        double inputFileSizeMB = inputFile.length() / (double) ODCSFusionToolUtils.MB_BYTES;

        DataSourceConfig dataSourceConfig = new DataSourceConfigImpl(EnumDataSourceType.FILE, "test-perf-file.nq");
        dataSourceConfig.getParams().put(ConfigParameters.DATA_SOURCE_FILE_PATH, inputFile.getAbsolutePath());
        dataSourceConfig.getParams().put(ConfigParameters.DATA_SOURCE_FILE_FORMAT, format.name());
        initStopwatch.stop();
        Set<AllTriplesLoader> dataSources = Collections.singleton(
                (AllTriplesLoader) new AllTriplesFileLoader(dataSourceConfig, ConfigConstants.DEFAULT_FILE_PARSER_CONFIG));

        System.out.printf("Initialized with %,d triples in %s\n", tripleCount, initStopwatch);
        System.out.printf("Using GZIP compression: %s (block size %d)\n", ExternalSortingInputLoader.USE_GZIP ? "yes" : "no", ExternalSortingInputLoader.GZIP_BUFFER_SIZE);

        // Act
        int actualTripleCount = 0;
        Stopwatch executionStopwatch;
        ExternalSortingInputLoader inputLoader = null;
        executionStopwatch = Stopwatch.createStarted();
        inputLoader = new ExternalSortingInputLoader(dataSources, testDir.getRoot(),
                ConfigConstants.DEFAULT_FILE_PARSER_CONFIG, maxMemorySize, false);
        try {
            inputLoader.initialize(new URIMappingIterableImpl());
            while (inputLoader.hasNext()) {
                actualTripleCount += inputLoader.nextQuads().size();
            }
            executionStopwatch.stop();
        } finally {
            inputLoader.close();
        }

        // Assert
        assertThat(actualTripleCount, equalTo(tripleCount));
        System.out.printf("Processed %,d triples of size %,.2f MB in %s\n", tripleCount, inputFileSizeMB, executionStopwatch);
    }
}
