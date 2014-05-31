package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.io.EnumSerializationFormat;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Various utility methods.
 */
public final class ODCSFusionToolAppUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ODCSFusionToolAppUtils.class);
    private static final String TEMP_FILE_SUFFIX = ".tmp";

    private static final long HOUR_MS = ODCSUtils.MILLISECONDS * ODCSUtils.TIME_UNIT_60 * ODCSUtils.TIME_UNIT_60;

    /** Number of bytes in a kilobyte. */
    public static final int KB_BYTES = 1024;

    /** Number of bytes in a megabyte. */
    public static final long MB_BYTES = 1024 * 1024;

    /** Number of bytes in a gigabyte. */
    public static final long GB_BYTES = 1024 * 1024 * 1024;

    /** Disable constructor for a utility class. */
    private ODCSFusionToolAppUtils() {
    }

    /**
     * Returns a human-readable (memory, file, ...) size.
     * @param byteCount the number of bytes
     * @return a human-readable size with units
     */
    public static String humanReadableSize(long byteCount) {
        String result;
        if (byteCount / GB_BYTES > 0) {
            result = Long.toString(byteCount / GB_BYTES) + " GB";
        } else if (byteCount / MB_BYTES > 0) {
            result = Long.toString(byteCount / MB_BYTES) + " MB";
        } else if (byteCount / KB_BYTES > 0) {
            result = Long.toString(byteCount / KB_BYTES) + " kB";
        } else {
            result = Long.toString(byteCount) + " B";
        }
        return result;
    }

    /**
     * Formats time in milliseconds.
     * @param timeInMs time in milliseconds
     * @return formatted string
     */
    public static String formatProfilingTime(long timeInMs) {
        DateFormat timeFormat = new SimpleDateFormat("mm:ss.SSS");
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return String.format(Locale.ROOT, "%d:%s (%d ms)",
                timeInMs / HOUR_MS,
                timeFormat.format(new Date(timeInMs)),
                timeInMs);
    }

    /**
     * Creates parent directories for the given file if they don't exist already.
     * @param file file whose parent directories will be created
     * @return the given file
     */
    public static File ensureParentsExists(File file) {
        file.getAbsoluteFile().getParentFile().mkdirs();
        return file;
    }

    /**
     * Creates a connection string for connecting to Virtuoso via JDBC.
     * @param host host for the connection
     * @param port connection port
     * @return JDBC connection string
     */
    public static String getVirtuosoConnectionString(String host, String port) {
        return "jdbc:virtuoso://" + host + ":" + port + "/CHARSET=UTF-8";
    }

    /**
     * Returns serialization format for the Sesame library.
     * @param serializationFormat user-supplied serialization format string; can be null
     * @param fileName file name used for automatic recognition of format when {@code serializationFormat} is null
     * @return Sesame serialization format
     */
    public static RDFFormat getSesameSerializationFormat(String serializationFormat, String fileName) {
        if (serializationFormat == null) {
            return RDFFormat.forFileName(fileName, EnumSerializationFormat.RDF_XML.toSesameFormat());
        } else {
            EnumSerializationFormat format = EnumSerializationFormat.parseFormat(serializationFormat);
            return format == null ? null : format.toSesameFormat();
        }
    }

    /**
     * Create a temporary file with in the given directory.
     * @param directory parent directory for the temporary file
     * @param filePrefix prefix for the file
     * @return reference to a new unique temporary file
     */

    public static File createTempFile(File directory, String filePrefix) throws IOException {
        ensureParentsExists(directory);
        File file = File.createTempFile(filePrefix, TEMP_FILE_SUFFIX, directory);
        LOG.debug("Creating temporary file {} in the working directory", file.getName());
        return file;
    }
}
