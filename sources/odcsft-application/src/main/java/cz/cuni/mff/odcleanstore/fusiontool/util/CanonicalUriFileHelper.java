package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.fusiontool.io.CountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Set;

/**
 * Reader and writer of files containing canonical URIs.
 * The implementation doesn't lock files but uses the move operation to ensure
 * files are not written to by different executions of a pipeline at the same time.
 * Note that this can lead to one pipeline overriding the results of another execution, however,
 * and therefore to loss of some canonical URIs.
 * @author Jan Michelfeit
 */
public class CanonicalUriFileHelper {
    private static final Logger LOG = LoggerFactory.getLogger(CanonicalUriFileHelper.class);

    /**
     * Read canonical URIs from a file with the given name and add them to the given Set.
     * Do nothing if canonicalUrisFileName is empty or the file doesn't exist.
     * @param canonicalUris set where to add loaded canonical URIs to
     * @throws IOException I/O error
     */
    public void readCanonicalUris(File canonicalUrisFile, Set<String> canonicalUris) throws IOException {
        if (canonicalUrisFile == null) {
            return;
        }
        if (canonicalUrisFile.isFile() && canonicalUrisFile.canRead()) {
            try (BufferedReader reader =
                         new BufferedReader(
                                 new InputStreamReader(
                                         new FileInputStream(canonicalUrisFile), "UTF-8"))) {
                long counter = 0;
                String line = reader.readLine();
                while (line != null) {
                    canonicalUris.add(line);
                    line = reader.readLine();
                    counter++;
                }
                LOG.info("Read {} canonical URIs from file '{}'", counter, canonicalUrisFile.getName());
            }
        } else if (canonicalUrisFile.exists()) {
            LOG.warn("Cannot read canonical URIs from '{}'. The file may have not been created yet.", canonicalUrisFile.getName());
            // Intentionally do not throw an exception
        }
    }

    /**
     * Write given canonical URIs to a file with the given name.
     * Do nothing if canonicalUrisFileName is empty.
     * @param canonicalUrisFile file with canonical URIs or null;
     *        this file will be looked up in the base directory given in constructor
     * @param canonicalUris canonical URIs to write
     * @throws IOException I/O error
     */
    public void writeCanonicalUris(File canonicalUrisFile, Set<String> canonicalUris) throws IOException {
        if (canonicalUrisFile == null) {
            return;
        }
        LDFusionToolUtils.ensureParentsExists(canonicalUrisFile);
        String canonicalUrisFileName = canonicalUrisFile.getName();
        File baseDirectory = canonicalUrisFile.getParentFile();
        long counter = 0;
        long byteCount = 0;
        File tmpFile = File.createTempFile("canon-" + canonicalUrisFileName, null, baseDirectory);
        try {
            try (CountingOutputStream outputStream = new CountingOutputStream(new FileOutputStream(tmpFile));
                 PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, "UTF-8"))) {
                for (String uri : canonicalUris) {
                    writer.println(uri);
                    counter++;
                }
                byteCount = outputStream.getByteCount();
            }

            try {
                Files.move(tmpFile.toPath(), canonicalUrisFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                LOG.info(String.format(
                        "Written %,d canonical URIs to file '%s' (total size %s)",
                        counter,
                        canonicalUrisFileName,
                        LDFusionToolUtils.humanReadableSize(byteCount)));
            } catch (IOException e) {
                LOG.error("Cannot write canonical URIs file {}", canonicalUrisFileName);
                // Intentionally do not throw an exception
            }
        } finally {
            tmpFile.delete();
        }
    }
}
