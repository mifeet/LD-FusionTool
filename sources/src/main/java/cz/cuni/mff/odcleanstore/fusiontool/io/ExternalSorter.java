package cz.cuni.mff.odcleanstore.fusiontool.io;

import cz.cuni.mff.odcleanstore.fusiontool.io.externalsort.ExternalSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;

/**
 * TODO
 */
public class ExternalSorter {
    private static final Logger LOG = LoggerFactory.getLogger(ExternalSorter.class);

    /**
     * Maximum number of temporary files to be created by external sort.
     * The input size divided by number of temporary files gives the needed memory size.
     * If the maximum number of files is too low, it may cause OutOfMemoryExceptions.
     */
    private static final int MAX_SORT_TMP_FILES = 2048;

    private static final Charset CHARSET = Charset.defaultCharset();

    private final Comparator<String> lineComparator;
    private final File cacheDirectory;
    private final boolean useGZip;
    private final long maxMemoryLimit;

    public ExternalSorter(Comparator<String> lineComparator, File cacheDirectory, boolean useGZip, long maxMemoryLimit) {
        this.lineComparator = lineComparator;
        this.cacheDirectory = cacheDirectory;
        this.useGZip = useGZip;
        this.maxMemoryLimit = maxMemoryLimit;
    }

    public void sort(BufferedReader inputReader, long inputSize, BufferedWriter outputWriter) throws IOException {
        List<File> sortFiles = ExternalSort.sortInBatch(
                inputReader,
                inputSize,
                lineComparator,
                MAX_SORT_TMP_FILES,
                maxMemoryLimit,
                CHARSET,
                cacheDirectory,
                true,
                0,
                useGZip);
        LOG.debug("Merging sorted data from {} blocks", sortFiles.size());
        ExternalSort.mergeSortedFiles(sortFiles,
                outputWriter,
                lineComparator,
                Charset.defaultCharset(),
                true, // distinct
                useGZip);
    }
}
