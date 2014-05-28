package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.fusiontool.io.EnumSerializationFormat;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Various utility methods.
 */
public final class ODCSFusionToolApplicationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ODCSFusionToolApplicationUtils.class);
    private static final String TEMP_FILE_SUFFIX = ".tmp";

    /** Disable constructor for a utility class. */
    private ODCSFusionToolApplicationUtils() {
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
