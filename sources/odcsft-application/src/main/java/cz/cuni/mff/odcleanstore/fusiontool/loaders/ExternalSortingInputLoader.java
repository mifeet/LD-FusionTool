package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import com.google.common.base.Preconditions;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.ResourceDescriptionImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.NTupleMergeTransformException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolApplicationException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.ExternalSorter;
import cz.cuni.mff.odcleanstore.fusiontool.io.ntuples.NTuplesFileMerger;
import cz.cuni.mff.odcleanstore.fusiontool.io.ntuples.NTuplesParser;
import cz.cuni.mff.odcleanstore.fusiontool.io.ntuples.NTuplesParserUtils;
import cz.cuni.mff.odcleanstore.fusiontool.io.ntuples.NTuplesWriter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.AtributeIndexFileNTuplesWriter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.DataFileAndAttributeIndexFileMerger;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.DataFileNTuplesWriter;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.ExternalSortingInputLoaderPreprocessor;
import cz.cuni.mff.odcleanstore.fusiontool.util.FederatedRDFHandler;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolAppUtils;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFParseException;
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
 * Method {@link #next()} can return descriptions of multiple resources at the same time,
 * however it is guaranteed that all returned descriptions are complete and sorted.
 */
public class ExternalSortingInputLoader implements InputLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ExternalSortingInputLoader.class);

    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
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
    private final File cacheDirectory;
    private final Long maxMemoryLimit;
    private final ParserConfig parserConfig;
    private final ExternalSorter externalSorter;
    private final Set<URI> canonicalResourceDescriptionProperties = new HashSet<>();
    private final Set<URI> _resourceDescriptionProperties;

    private NTuplesParser dataFileIterator;
    private NTuplesParser mergedAttributeFileIterator;
    private final Collection<File> temporaryFiles = new ArrayList<File>();

    /**
     * @param dataSources initialized {@link cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesLoader} loaders
     * @param cacheDirectory directory for temporary files
     * @param parserConfig RDF parser configuration
     * @param maxMemoryLimit maximum memory amount to use for large operations;
* if the limit is too high, it may cause OutOfMemory exceptions
     */
    public ExternalSortingInputLoader(
            Collection<AllTriplesLoader> dataSources,
            Set<URI> resourceDescriptionProperties,
            File cacheDirectory,
            ParserConfig parserConfig,
            long maxMemoryLimit) {

        Preconditions.checkNotNull(dataSources);
        Preconditions.checkNotNull(cacheDirectory);
        Preconditions.checkNotNull(resourceDescriptionProperties);
        this.dataSources = dataSources;
        this._resourceDescriptionProperties = resourceDescriptionProperties;
        this.maxMemoryLimit = maxMemoryLimit;
        this.cacheDirectory = cacheDirectory;
        this.parserConfig = parserConfig;
        this.externalSorter = new ExternalSorter(getSortComparator(), cacheDirectory, USE_GZIP, maxMemoryLimit);
    }

    @Override
    public void initialize(UriMappingIterable uriMapping) throws ODCSFusionToolException {
        Preconditions.checkNotNull(uriMapping);

        LOG.info("Initializing input loader");
        if (maxMemoryLimit < Long.MAX_VALUE) {
            LOG.info("  maximum memory limit is {} MB", String.format("%,.2f", maxMemoryLimit / (double) ODCSFusionToolAppUtils.MB_BYTES));
        }

        canonicalResourceDescriptionProperties.clear();
        for (URI resourceDescriptionProperty : _resourceDescriptionProperties) {
            canonicalResourceDescriptionProperties.add((URI) uriMapping.mapResource(resourceDescriptionProperty));
        }

        try {
            // Will contain c(S) S P O G for input quads (S,P,O,G)
            // c(x) means canonical version of x
            File dataFile = createTempFile();
            // Will contain c(O) c(S) for input quads (S,P,O,G) such that P is a resource description URI and O is a {@link org.openrdf.model.Resource}
            File attributeIndexFile = createTempFile();

            copyInputsToTempFiles(dataSources, uriMapping, dataFile, attributeIndexFile);

            File sortedDataFile = sortAndDeleteFile(dataFile);
            File sortedAttributeIndexFile = sortAndDeleteFile(attributeIndexFile);

            // Will contain c(E) S P O G for input quads (E,P',S,G') (S,P,O,G) such that P' is a resource description URI
            File mergedAttributeFile = createTempFile();
            NTuplesFileMerger fileMerger = new NTuplesFileMerger(new DataFileAndAttributeIndexFileMerger(), parserConfig);
            fileMerger.merge(
                    createTempFileReader(sortedDataFile),
                    createTempFileReader(sortedAttributeIndexFile),
                    createTempFileWriter(mergedAttributeFile));
            sortedAttributeIndexFile.delete();
            File sortedMergedAttributeFile = sortAndDeleteFile(mergedAttributeFile); // TODO: test

            dataFileIterator = createParserIteratorFromSortedFile(sortedDataFile);
            mergedAttributeFileIterator = createParserIteratorFromSortedFile(sortedMergedAttributeFile);

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
    public ResourceDescription next() throws ODCSFusionToolException {
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
            List<Value> nextTuple = dataFileIterator.next();
            Statement firstStatement = createStatement(nextTuple);
            Resource firstCanonicalSubject = (Resource) nextTuple.get(0);

            // Add quads for the cluster from primary data file
            describingStatements.add(firstStatement);
            while (NTuplesParserUtils.hasMatchingRecord(dataFileIterator, firstCanonicalSubject)) {
                describingStatements.add(createStatement(dataFileIterator.next()));
            }

            // Add additional quads from other files
            int extendedDescriptionCount = 0;
            boolean foundMatch = NTuplesParserUtils.skipLessThan(mergedAttributeFileIterator, firstCanonicalSubject, NTuplesParserUtils.VALUE_COMPARATOR);
            if (foundMatch) {
                while (NTuplesParserUtils.hasMatchingRecord(mergedAttributeFileIterator, firstCanonicalSubject)) {
                    describingStatements.add(createStatement(mergedAttributeFileIterator.next()));
                    extendedDescriptionCount++;
                }
            }

            LOG.debug("Loaded {} quads for resource <{}> (including {} triples in extended description)",
                    new Object[]{describingStatements.size(), firstCanonicalSubject, extendedDescriptionCount});

            return new ResourceDescriptionImpl(firstCanonicalSubject, describingStatements);
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
        LOG.debug("Deleting input loader temporary files");
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
     * <li> c(S) S P O G for input quads (S,P,O,G) to {@code dataFile}</li>
     * <li> c(O) c(S) for input quads (S,P,O,G) such that P is a resource description URI to {@code attributeIndexFile} and O is a {@link org.openrdf.model.Resource}</li>
     * </ul>
     * where c(x) is the canonical version of x.
     */
    private void copyInputsToTempFiles(Collection<AllTriplesLoader> dataSources, UriMappingIterable uriMapping, File dataFile, File attributeIndexFile)
            throws ODCSFusionToolException {
        NTuplesWriter dataFileWriter = null;
        NTuplesWriter attributeIndexFileWriter = null;
        try {
            dataFileWriter = new NTuplesWriter(createTempFileWriter(dataFile));
            attributeIndexFileWriter = new NTuplesWriter(createTempFileWriter(attributeIndexFile));
            RDFHandler tempFilesWriteHandler = new FederatedRDFHandler(
                    new DataFileNTuplesWriter(dataFileWriter, uriMapping),
                    new AtributeIndexFileNTuplesWriter(attributeIndexFileWriter, canonicalResourceDescriptionProperties, uriMapping));

            ExternalSortingInputLoaderPreprocessor inputLoaderPreprocessor = new ExternalSortingInputLoaderPreprocessor(
                    tempFilesWriteHandler, VF);

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
        LOG.debug("Sorting temporary file");
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
        return SortComparator.INSTANCE;
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

    private static class SortComparator implements Comparator<String> {
        public static final SortComparator INSTANCE = new SortComparator();

        @Override
        public int compare(String r1, String r2) {
            try {
                Resource resource1 = NTuplesParserUtils.parseValidResource(r1);
                Resource resource2 = NTuplesParserUtils.parseValidResource(r2);
                return NTuplesParserUtils.VALUE_COMPARATOR.compare(resource1, resource2);
            } catch (RDFParseException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
