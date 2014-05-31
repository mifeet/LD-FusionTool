package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import com.google.common.base.Preconditions;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.ValueComparator;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigConstants;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.ResourceDescriptionImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.NTupleMergeTransformException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolApplicationException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.*;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.AtributeIndexFileNTuplesWriter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.DataFileAndAttributeIndexFileMerger;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.DataFileNTuplesWriter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.ExternalSortingInputLoaderPreprocessor;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.util.FederatedRDFHandler;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolAppUtils;
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
public class ExternalSortingInputLoader implements InputLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ExternalSortingInputLoader.class);

    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
    private static final ValueComparator VALUE_COMPARATOR = new ValueComparator();
    private static final String TEMP_FILE_PREFIX = "odcs-ft.sort-loader.";
    private static final Charset CHARSET = Charset.defaultCharset();

    /**
     * Indicates whether to use gzip compression in temporary files.
     * even though the performance is a little worse.
     * Set to true to save spaces when deployed e.g. as a DPU running as part of ODCS/UnifiedViews framework.
     */
    public static final boolean USE_GZIP = true;

    /**
     * Buffer size for gzip compression.
     */
    static final int GZIP_BUFFER_SIZE = 2048;

    private final Collection<AllTriplesLoader> dataSources;
    private final boolean outputMappedSubjectsOnly;
    private final File cacheDirectory;
    private final Long maxMemoryLimit;
    private final ParserConfig parserConfig;
    private final ExternalSorter externalSorter;

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
    public ExternalSortingInputLoader(
            Collection<AllTriplesLoader> dataSources,
            File cacheDirectory,
            ParserConfig parserConfig,
            long maxMemoryLimit,
            boolean outputMappedSubjectsOnly) {

        Preconditions.checkNotNull(dataSources);
        Preconditions.checkNotNull(cacheDirectory);
        this.dataSources = dataSources;
        this.outputMappedSubjectsOnly = outputMappedSubjectsOnly;
        this.maxMemoryLimit = maxMemoryLimit;
        this.cacheDirectory = cacheDirectory;
        this.parserConfig = parserConfig;
        this.externalSorter = new ExternalSorter(getSortComparator(), cacheDirectory, USE_GZIP, maxMemoryLimit);
    }

    @Override
    public void initialize(URIMappingIterable uriMapping) throws ODCSFusionToolException {
        Preconditions.checkNotNull(uriMapping);
        LOG.info("Initializing input loader");
        if (maxMemoryLimit < Long.MAX_VALUE) {
            LOG.info("  maximum memory limit is {} MB", String.format("%,.2f", maxMemoryLimit / (double) ODCSFusionToolAppUtils.MB_BYTES));
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
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.INPUT_LOADER_TMP_FILE_INIT, "Error creating temporary files in input loader", e);
        } catch (NTupleMergeTransformException e) {
            closeOnException();
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.INPUT_LOADER_MERGE, "Unexpected error when merging temporary files", e);
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
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.INPUT_LOADER_LOADING,
                    "Error when reading temporary file in input loader", e);
        }
    }

    @Override
    public ResourceDescription nextQuads() throws ODCSFusionToolException {
        if (dataFileIterator == null || mergedAttributeFileIterator == null) {
            throw new IllegalStateException();
        }
        try {
            if (!dataFileIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            ArrayList<Statement> describingStatements = new ArrayList<Statement>();

            // Read next record from dataFileIterator which represents the primary file
            // - the subject will determine the next cluster
            Statement firstStatement = createStatement(dataFileIterator.next());
            Resource firstSubject = firstStatement.getSubject();

            // Add quads for the cluster from primary data file
            describingStatements.add(firstStatement);
            while (NTuplesParserUtils.hasMatchingRecord(dataFileIterator, firstSubject)) {
                describingStatements.add(createStatement(dataFileIterator.next()));
            }

            // Add additional quads from other files
            int extendedDescriptionCount = 0;
            boolean foundMatch = NTuplesParserUtils.skipLessThan(mergedAttributeFileIterator, firstSubject, VALUE_COMPARATOR);
            if (foundMatch) {
                while (NTuplesParserUtils.hasMatchingRecord(mergedAttributeFileIterator, firstSubject)) {
                    describingStatements.add(createStatement(mergedAttributeFileIterator.next()));
                    extendedDescriptionCount++;
                }
            }

            LOG.debug("Loaded {} quads for resource <{}> (including {} triples in extended description)",
                    new Object[]{describingStatements.size(), firstSubject, extendedDescriptionCount});

            return new ResourceDescriptionImpl(firstSubject, describingStatements);
        } catch (Exception e) {
            closeOnException();
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.INPUT_LOADER_HAS_NEXT,
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

            ExternalSortingInputLoaderPreprocessor inputLoaderPreprocessor = new ExternalSortingInputLoaderPreprocessor(
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
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.INPUT_LOADER_TMP_FILE_INIT,
                    "Error while writing quads to temporary file in input loader", e);
        } finally {
            tryCloseWriter(dataFileWriter);
            tryCloseWriter(attributeIndexFileWriter);
        }
    }

    private Statement createStatement(List<Value> tuple) throws ODCSFusionToolException {
        if (tuple == null || tuple.size() < 4) {
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.INVALID_TMP_FILE_FORMAT_TUPLE,
                    "Invalid format of temporary file, expected statement but found: " + tuple);
        }
        int size = tuple.size();
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
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.INVALID_TMP_FILE_FORMAT, message);
        }
    }

    private File sortAndDeleteFile(File inputFile) throws ODCSFusionToolException {
        File sortedFile = sortFile(inputFile);
        inputFile.delete();
        return sortedFile;
    }

    private File sortFile(File inputFile) throws ODCSFusionToolException {
        // External sort the temporary file
        LOG.info("Sorting temporary file");
        try {
            long startTime = System.currentTimeMillis();
            File sortedFile = createTempFile();
            BufferedReader reader = createTempFileReader(inputFile);
            BufferedWriter writer = createTempFileWriter(sortedFile);

            externalSorter.sort(reader, inputFile.length(), writer);
            LOG.debug("Sorting finished in {}", ODCSFusionToolAppUtils.formatProfilingTime(System.currentTimeMillis() - startTime));
            return sortedFile;
        } catch (IOException e) {
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.INPUT_LOADER_SORT,
                    "Error while sorting quads in input loader", e);
        }
    }

    private static Comparator<String> getSortComparator() {
        // compare lines as string, which works fine for NQuads/NTuples
        // TODO: NTuples files only by the first component?
        return new Comparator<String>() {
            @Override
            public int compare(String r1, String r2) {
                return r1.compareTo(r2);
            }
        };
    }

    private File createTempFile() throws IOException {
        File tempFile = ODCSFusionToolAppUtils.createTempFile(cacheDirectory, TEMP_FILE_PREFIX);
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

    private static BufferedWriter createTempFileWriter(File file) throws IOException {
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
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.INPUT_LOADER_PARSE_TEMP_FILE,
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
