package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import com.google.code.externalsorting.ExternalSort;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.SpogComparator;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.ValueComparator;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigConstants;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.NTupleMergeTransformException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesFileMerger;
import cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesParser;
import cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesWriter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.AtributeIndexFileNTuplesWriter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.DataFileAndAttributeIndexFileMerger;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.DataFileNTuplesWriter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.ExternalSortingInputLoaderPreprocessor3;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.util.FederatedRDFHandler;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFHandler;
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
public class ExternalSortingInputLoader3 implements InputLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ExternalSortingInputLoader3.class);

    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
    private static final ValueComparator VALUE_COMPARATOR = new ValueComparator();
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
     * even though the performanc is a little lower.
     * Set to true to save spaces when deployed e.g. as a DPU running as part of ODCS/UnifiedViews framework.
     */
    public static final boolean USE_GZIP = true;

    /**
     * Buffer size for gzip compression.
     */
    private static final int GZIP_BUFFER_SIZE = 2048;

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

    private NTuplesParser dataFileIterator;
    private NTuplesParser mergedAttributeFileIterator;
    private final Collection<File> temporaryFiles = new ArrayList<File>();

    /**
     * @param dataSources initialized {@link cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesLoader} loaders
     * @param cacheDirectory directory for temporary files
     * @param parserConfig RDF parser configuration
     * @param maxMemoryLimit maximum memory amount to use for large operations;
     * if the limit is too high, it may cause OutOfMemory exceptions
     * @param outputMappedSubjectsOnly see {@link cz.cuni.mff.odcleanstore.fusiontool.config.Config#getOutputMappedSubjectsOnly()}
     */
    public ExternalSortingInputLoader3(
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
            // Will contain S P O G for input quads (S,P,O,G)
            File dataFile = createTempFile();
            // Will contain O S for input quads (S,P,O,G) such that P is a resource description URI
            File attributeIndexFile = createTempFile();

            copyInputsToTempFiles(dataSources, uriMapping, dataFile, attributeIndexFile);

            File sortedDataFile = sortAndDeleteFile(dataFile);
            File sortedAttributeIndexFile = sortAndDeleteFile(attributeIndexFile);

            // Will contain E S P O G for input quads (E,P',S,G') (S,P,O,G) such that P' is a resource description URI
            File mergedAttributeFile = createTempFile();
            NTuplesFileMerger fileMerger = new NTuplesFileMerger(new DataFileAndAttributeIndexFileMerger(), parserConfig);
            fileMerger.merge(
                    createTempFileReader(sortedDataFile),
                    createTempFileReader(sortedAttributeIndexFile),
                    createTempFileWriter(mergedAttributeFile));
            sortedAttributeIndexFile.delete();

            dataFileIterator = createParserIteratorFromSortedFile(sortedDataFile);
            mergedAttributeFileIterator = createParserIteratorFromSortedFile(mergedAttributeFile);

            LOG.info("Input loader initialization finished");
        } catch (ODCSFusionToolException e) {
            closeOnException();
            throw e;
        } catch (IOException e) {
            closeOnException();
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_TMP_FILE_INIT, "Error creating temporary files in input loader", e);
        } catch (NTupleMergeTransformException e) {
            closeOnException();
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_MERGE, "Unexpected error when merging temporary files", e);
        }
    }

    @Override
    public boolean hasNext() throws ODCSFusionToolException {
        if (dataFileIterator == null) {
            throw new IllegalStateException();
        }
        try {
            return dataFileIterator.hasNext();
        } catch (Exception e) {
            closeOnException();
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_LOADING,
                    "Error when reading temporary file in input loader", e);
        }
    }

    @Override
    public Collection<Statement> nextQuads() throws ODCSFusionToolException {
        if (dataFileIterator == null || mergedAttributeFileIterator == null) {
            throw new IllegalStateException();
        }
        try {
            if (!dataFileIterator.hasNext()) {
                return Collections.emptySet();
            }
            ArrayList<Statement> result = new ArrayList<Statement>();

            // Read next record from dataFileIterator which represents the primary file
            // - the subject will determine the next cluster
            Statement firstStatement = createStatement(dataFileIterator.next());
            Resource firstSubject = firstStatement.getSubject();

            // Add quads for the cluster from primary data file
            result.add(firstStatement);
            while (hasMatchingRecord(dataFileIterator, firstSubject)) {
                result.add(createStatement(dataFileIterator.next()));
            }

            // Add additional quads from other files
            int extendedDescriptionCount = 0;
            boolean foundMatch = skipLessThan(mergedAttributeFileIterator, firstSubject);
            if (foundMatch) {
                while (hasMatchingRecord(mergedAttributeFileIterator, firstSubject)) {
                    result.add(createStatement(mergedAttributeFileIterator.next()));
                    extendedDescriptionCount++;
                }
            }

            LOG.debug("Loaded {} quads for resource <{}> (including {} triples in extended description)",
                    new Object[]{result.size(), firstSubject, extendedDescriptionCount});
            return result;
        } catch (Exception e) {
            closeOnException();
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_HAS_NEXT,
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
        if (dataFileIterator != null) {
            try {
                dataFileIterator.close();
                dataFileIterator = null;
            } catch (IOException e) {
                // ignore
            }
        }
        if (mergedAttributeFileIterator != null) {
            try {
                mergedAttributeFileIterator.close();
                mergedAttributeFileIterator = null;
            } catch (IOException e) {
                // ignore
            }
        }

        // Delete temporary files
        for (File temporaryFile : temporaryFiles) {
            try {
                if (temporaryFile.exists()) {
                    temporaryFile.delete();
                }
            } catch (Exception e) {
                LOG.error("Error deleting temporary file " + temporaryFile.getName(), e);
            }
        }
        temporaryFiles.clear();
    }

    /**
     * Reads all input quads and outputs them to temporary files.
     * Data are written to temporary files in the following format:
     * <ul>
     * <li> S P O G for input quads (S,P,O,G) to {@code dataFile}</li>
     * <li> O S for input quads (S,P,O,G) such that P is a resource description URI to {@code attributeIndexFile}</li>
     * </ul>
     */
    private void copyInputsToTempFiles(Collection<AllTriplesLoader> dataSources, URIMappingIterable uriMapping, File dataFile, File attributeIndexFile)
            throws ODCSFusionToolException {
        NTuplesWriter dataFileWriter = null;
        NTuplesWriter attributeIndexFileWriter = null;
        try {
            dataFileWriter = new NTuplesWriter(createTempFileWriter(dataFile));
            attributeIndexFileWriter = new NTuplesWriter(createTempFileWriter(attributeIndexFile));
            RDFHandler tempFilesWriteHandler = new FederatedRDFHandler(
                    new DataFileNTuplesWriter(dataFileWriter),
                    new AtributeIndexFileNTuplesWriter(attributeIndexFileWriter, ConfigConstants.RESOURCE_DESCRIPTION_URIS));

            ExternalSortingInputLoaderPreprocessor3 inputLoaderPreprocessor = new ExternalSortingInputLoaderPreprocessor3(
                    tempFilesWriteHandler, uriMapping, VF, outputMappedSubjectsOnly);

            tempFilesWriteHandler.startRDF();
            for (AllTriplesLoader dataSource : dataSources) {
                try {
                    inputLoaderPreprocessor.setDefaultContext(dataSource.getDefaultContext());
                    dataSource.loadAllTriples(inputLoaderPreprocessor);
                    // TODO: Presort in memory (presort NTuples wrapper) & do not write to disc if small enough?
                } finally {
                    dataSource.close();
                }
            }
            tempFilesWriteHandler.endRDF();
        } catch (Exception e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_TMP_FILE_INIT,
                    "Error while writing quads to temporary file in input loader", e);
        } finally {
            tryCloseWriter(dataFileWriter);
            tryCloseWriter(attributeIndexFileWriter);
        }
    }

    private Statement createStatement(List<Value> tuple) throws ODCSFusionToolException {
        int size = tuple.size();
        if (tuple == null || size < 4) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INVALID_TMP_FILE_FORMAT_TUPLE,
                    "Invalid format of temporary file, expected statement but found: " + tuple);
        }
        try {
            // Take the last four elements from the tuple
            return VF.createStatement(
                    (Resource) tuple.get(size - 4),
                    (URI) tuple.get(size - 3),
                    tuple.get(size - 2),
                    (Resource) tuple.get(size - 1));
        } catch (ClassCastException e) {
            String message = "Invalid format of temporary file, expected statement but found: "
                    + tuple.subList(tuple.size() - 4, tuple.size());
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INVALID_TMP_FILE_FORMAT, message);
        }
    }

    // TODO: move to NTuplesParser utils
    private boolean hasMatchingRecord(NTuplesParser parser, Value comparedValue) throws IOException {
        return parser.hasNext() && !parser.peek().isEmpty() && parser.peek().get(0).equals(comparedValue);
    }

    /**
     * Skips all records in the parser that have the first component of the tuple less than compared value.
     * After execution of this method, {@code parser} will point to the first record with first component greater
     * or equal to {@code comparedValue} or beyond the end of file if there are no more records
     * @param parser parser to move forward
     * @param comparedValue compared value
     * @return true if the value returned by {@code parser.next()} will be equal to {@code comparedValue}, false otherwise
     * @throws IOException I/O error
     */
    private boolean skipLessThan(NTuplesParser parser, Value comparedValue) throws IOException {
        // TODO: move to NTuplesParser utils
        int cmp = -1;
        while (parser.hasNext() && !parser.peek().isEmpty()
                && (cmp = VALUE_COMPARATOR.compare(parser.peek().get(0), comparedValue)) < 0) {
            parser.next();
        }
        return cmp == 0;
    }

    private File sortAndDeleteFile(File inputFile) throws ODCSFusionToolException {
        File sortedFile = sortFile(inputFile);
        inputFile.delete();
        return sortedFile;
    }

    private File sortFile(File inputFile) throws ODCSFusionToolException {
        // TODO: move to utility class
        // External sort the temporary file
        LOG.info("Sorting temporary file (size on disk {} MB)",
                String.format("%,.2f", inputFile.length() / (double) ODCSFusionToolUtils.MB_BYTES));
        try {
            long startTime = System.currentTimeMillis();
            File sortedFile = createTempFile();
            Comparator<String> comparator = getSortComparator();

            BufferedReader reader = createTempFileReader(inputFile);
            List<File> sortFiles = ExternalSort.sortInBatch(
                    reader,
                    inputFile.length(),
                    comparator,
                    MAX_SORT_TMP_FILES,
                    maxMemoryLimit,
                    CHARSET,
                    cacheDirectory,
                    true,
                    0,
                    USE_GZIP);
            LOG.debug("... merging sorted data from {} blocks", sortFiles.size());
            ExternalSort.mergeSortedFiles(sortFiles,
                    sortedFile,
                    comparator,
                    Charset.defaultCharset(),
                    true, // distinct
                    false,
                    USE_GZIP);
            LOG.debug("Sorting finished in {}", ODCSFusionToolUtils.formatProfilingTime(System.currentTimeMillis() - startTime));
            return sortedFile;
        } catch (IOException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_SORT,
                    "Error while sorting quads in input loader", e);
        }
    }

    private Comparator<String> getSortComparator() {
        // compare lines as string, which works fine for NQuads/NTuples
        // TODO: NTuples files only by the first component?
        return ExternalSort.defaultcomparator;
    }

    private File createTempFile() throws IOException {
        File tempFile = ODCSFusionToolUtils.createTempFile(cacheDirectory, TEMP_FILE_PREFIX);
        temporaryFiles.add(tempFile); // register it so that we don't forget to delete it
        return tempFile;
    }

    private static BufferedReader createTempFileReader(File file) throws IOException {
        InputStream inputStream = new FileInputStream(file);
        if (USE_GZIP) {
            inputStream = new GZIPInputStream(inputStream, GZIP_BUFFER_SIZE);
        }
        // BOMInputStream ?
        return new BufferedReader(new InputStreamReader(inputStream, CHARSET));
    }

    private static Writer createTempFileWriter(File file) throws IOException {
        OutputStream outputStream = new FileOutputStream(file);
        if (USE_GZIP) {
            outputStream = new GZIPOutputStream(outputStream, GZIP_BUFFER_SIZE) {
                {
                    this.def.setLevel(Deflater.BEST_SPEED);
                }
            };
        }
        return new BufferedWriter(new OutputStreamWriter(outputStream, CHARSET));
    }

    private NTuplesParser createParserIteratorFromSortedFile(File sortedTempFile) throws ODCSFusionToolException {
        try {
            return new NTuplesParser(createTempFileReader(sortedTempFile), parserConfig);
        } catch (IOException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_PARSE_TEMP_FILE,
                    "Error while initializing temporary file reader in input loader", e);
        }
    }

    private void tryCloseWriter(NTuplesWriter writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Error closing output writer", e);
            }
        }
    }

    private void closeOnException() {
        try {
            close(); // clean up temporary files defensively in case the caller doesn't call close() on exception
        } catch (Exception e2) {
            // ignore
        }
    }
}
