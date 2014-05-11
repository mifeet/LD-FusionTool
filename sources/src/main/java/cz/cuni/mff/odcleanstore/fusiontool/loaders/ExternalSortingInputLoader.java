package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import com.google.code.externalsorting.ExternalSort;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.SpogComparator;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.NQuadsParserIterator;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Input loader performing an external sort on the input quads in order
 * to efficiently process large amounts of quads.
 * The loader (1) reads all input quads, (2) maps URIs to canonical URIs,
 * (3) sorts quads using external sort, (4) iterates over large chunks of the sorted quads.
 * Method {@link #nextQuads()} can return descriptions of multiple resources at the same time,
 * however it is guaranteed that all returned descriptions are complete and sorted.
 */
public class ExternalSortingInputLoader implements InputLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ExternalSortingInputLoader.class);

    private static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();
    private static final RDFFormat TEMP_FILE_SERIALIZATION = RDFFormat.NQUADS;
    private static final String TEMP_FILE_PREFIX = "odcs-ft.sort-loader.";
    private static final Charset CHARSET = Charset.defaultCharset();

    /**
     * Maximum number of temporary files to be created by external sort.
     * The input size divided by number of temporary files gives the needed memory size.
     * If the maximum number of files is too low, it may cause OutOfMemoryExceptions.
     */
    private static final int MAX_SORT_TMP_FILES = 2048;

    /**
     * Indicates whether to use gzip compression in temporary files.
     */
    public static final boolean USE_GZIP = false;

    public static final int GZIP_BUFFER_SIZE = 2048;

    /**
     * Comparator used for statement preprocessing.
     * Comparing by subject (and predicate) would be enough but sorting by spog
     * alleviates the string-based external sort.
     */
    private static final Comparator<Statement> ORDER_COMPARATOR = new SpogComparator();


    private final Collection<AllTriplesLoader> dataSources;
    private final boolean outputMappedSubjectsOnly;
    private final File cacheDirectory;
    private final Long maxMemoryLimit;
    private final ParserConfig parserConfig;

    private File tempSortedFile = null;
    private File tempInputFile = null;
    private NQuadsParserIterator quadIterator;

    /**
     * @param dataSources initialized {@link AllTriplesLoader} loaders
     * @param cacheDirectory directory for temporary files
     * @param parserConfig RDF parser configuration
     * @param maxMemoryLimit maximum memory amount to use for large operations;
     *      if the limit is too high, it may cause OutOfMemory exceptions
     * @param outputMappedSubjectsOnly see {@link cz.cuni.mff.odcleanstore.fusiontool.config.Config#getOutputMappedSubjectsOnly()}
     */
    public ExternalSortingInputLoader(
            Collection<AllTriplesLoader> dataSources,
            File cacheDirectory,
            ParserConfig parserConfig,
            long maxMemoryLimit,
            boolean outputMappedSubjectsOnly) {

        ODCSFusionToolUtils.checkNotNull(dataSources);
        ODCSFusionToolUtils.checkNotNull(cacheDirectory);
        this.dataSources = dataSources;
        this.outputMappedSubjectsOnly = outputMappedSubjectsOnly;
        this.maxMemoryLimit = maxMemoryLimit;
        this.cacheDirectory = cacheDirectory;
        this.parserConfig = parserConfig;
    }


    @Override
    public void initialize(URIMappingIterable uriMapping) throws ODCSFusionToolException {
        ODCSFusionToolUtils.checkNotNull(uriMapping);
        LOG.info("Initializing input loader");
        if (maxMemoryLimit < Long.MAX_VALUE) {
            LOG.info("  maximum memory limit is {} MB", String.format("%,.2f", maxMemoryLimit / (double) ODCSFusionToolUtils.MB_BYTES));
        }

        try {
            copyInputsToTempInputFile(uriMapping);
            sortTempInputFileToTempSortedFile();
            dropTempInputFile(); // no longer needed, remove asap
            initializeQuadIteratorFromTempSortedFile();
        } catch (ODCSFusionToolException e) {
            closeOnException();
            throw e;
        }
    }

    @Override
    public Collection<Statement> nextQuads() throws ODCSFusionToolException {
        if (quadIterator == null) {
            throw new IllegalStateException();
        }
        try {
            if (!quadIterator.hasNext()) {
                return Collections.emptySet();
            }
            ArrayList<Statement> result = new ArrayList<Statement>();
            Statement first = quadIterator.next();
            Resource firstSubject = first.getSubject();
            result.add(first);
            while (quadIterator.hasNext() && quadIterator.peek().getSubject().equals(firstSubject)) {
                result.add(quadIterator.next());
            }
            LOG.debug("Loaded {} quads for resource <{}>", result.size(), firstSubject);
            return result;
        } catch (Exception e) {
            closeOnException();
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_HAS_NEXT,
                    "Error when reading temporary file in input loader", e);
        }
    }

    @Override
    public boolean hasNext() throws ODCSFusionToolException {
        if (quadIterator == null) {
            throw new IllegalStateException();
        }
        try {
            return quadIterator.hasNext();
        } catch (Exception e) {
            closeOnException();
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_LOADING,
                    "Error when reading temporary file in input loader", e);
        }
    }

    @Override
    public void updateWithResolvedStatements(Collection<ResolvedStatement> resolvedStatements) {
        // do nothing
    }

    @Override
    public void close() throws ODCSFusionToolException {
        LOG.info("Deleting input loader temporary files");
        if (quadIterator != null) {
            try {
                quadIterator.close();
                quadIterator = null;
            } catch (IOException e) {
                // ignore
            }
        }
        if (tempInputFile != null && tempInputFile.exists()) {
            tempInputFile.delete();
            tempInputFile = null;
        }
        if (tempSortedFile != null && tempSortedFile.exists()) {
            tempSortedFile.delete();
            tempSortedFile = null;
        }
    }

    /** Read all inputs and write quads to a temporary file. */
    private void copyInputsToTempInputFile(URIMappingIterable uriMapping) throws ODCSFusionToolException {
        Writer tempOutputWriter = null;
        try {
            tempInputFile = ODCSFusionToolUtils.createTempFile(cacheDirectory, TEMP_FILE_PREFIX);
            tempOutputWriter = createTempFileWriter(tempInputFile, USE_GZIP);
            RDFWriter tempRdfWriter = Rio.createWriter(TEMP_FILE_SERIALIZATION, tempOutputWriter);
            ExternalSortingInputLoaderPreprocessor inputLoaderPreprocessor = new ExternalSortingInputLoaderPreprocessor(
                    uriMapping,
                    tempRdfWriter,
                    maxMemoryLimit,
                    VALUE_FACTORY,
                    ORDER_COMPARATOR,
                    outputMappedSubjectsOnly);

            tempRdfWriter.startRDF();
            for (AllTriplesLoader dataSource : dataSources) {
                try {
                    inputLoaderPreprocessor.setDefaultContext(dataSource.getDefaultContext());
                    dataSource.loadAllTriples(inputLoaderPreprocessor);
                } finally {
                    dataSource.close();
                }
            }
            tempRdfWriter.endRDF();
            tempOutputWriter.close();
        } catch (ODCSFusionToolException e) {
            throw e;
        } catch (Exception e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_TMP_FILE_INIT,
                    "Error while writing quads to temporary file in input loader", e);
        } finally {
            if (tempOutputWriter != null) {
                try {
                    tempOutputWriter.close();
                } catch (IOException e) {
                    LOG.error("Error closing output writer", e);
                }
            }
        }
    }

    private void sortTempInputFileToTempSortedFile() throws ODCSFusionToolException {
        // External sort the temporary file
        try {
            LOG.info("Sorting temporary data file (size on disk {} MB)",
                    String.format("%,.2f", tempInputFile.length() / (double) ODCSFusionToolUtils.MB_BYTES));
            long startTime = System.currentTimeMillis();
            tempSortedFile = ODCSFusionToolUtils.createTempFile(cacheDirectory, TEMP_FILE_PREFIX);
            // compare lines as string, which works fine for NQuads;
            // the only requirement is that quads with the same subject and predicate are
            // sorted as adjacent
            Comparator<String> comparator = ExternalSort.defaultcomparator;
            BufferedReader reader = createTempFileReader(tempInputFile, USE_GZIP);
            List<File> sortFiles = ExternalSort.sortInBatch(
                    reader,
                    tempInputFile.length(),
                    comparator,
                    MAX_SORT_TMP_FILES,
                    maxMemoryLimit,
                    CHARSET,
                    cacheDirectory,
                    true,
                    0,
                    USE_GZIP);
            LOG.debug(" merging sorted data from {} blocks", sortFiles.size());
            ExternalSort.mergeSortedFiles(sortFiles,
                    tempSortedFile,
                    comparator,
                    Charset.defaultCharset(),
                    true, // distinct
                    false,
                    USE_GZIP);
            LOG.debug("Sorting finished in {}", ODCSFusionToolUtils.formatProfilingTime(System.currentTimeMillis() - startTime));
        } catch (IOException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_SORT,
                    "Error while sorting quads in input loader", e);
        }
    }

    private void initializeQuadIteratorFromTempSortedFile() throws ODCSFusionToolException {
        try {
            quadIterator = new NQuadsParserIterator(createTempFileReader(tempSortedFile, false), parserConfig);
        } catch (IOException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_PARSE_TEMP_FILE,
                    "Error while initializing temporary file reader in input loader", e);
        }
    }

    private static BufferedReader createTempFileReader(File file, boolean useGzip) throws IOException {
        InputStream inputStream = new FileInputStream(file);
        if (useGzip) {
            inputStream = new GZIPInputStream(inputStream, GZIP_BUFFER_SIZE);
        }
        // BOMInputStream ?
        return new BufferedReader(new InputStreamReader(inputStream, CHARSET));
    }

    private static Writer createTempFileWriter(File file, boolean useGzip) throws IOException {
        OutputStream outputStream = new FileOutputStream(file);
        if (useGzip) {
            outputStream = new GZIPOutputStream(outputStream, GZIP_BUFFER_SIZE) {
                {
                    this.def.setLevel(Deflater.BEST_SPEED);
                }
            };
        }
        return new BufferedWriter(new OutputStreamWriter(outputStream, CHARSET));
    }

    private void dropTempInputFile() {
        tempInputFile.delete();
        tempInputFile = null;
    }

    private void closeOnException() {
        try {
            close(); // clean up temporary files just in case the caller doesn't call close() on exception
        } catch (Exception e2) {
            // ignore
        }
    }
}
