package cz.cuni.mff.odcleanstore.fusiontool.util;

import java.io.File;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.io.EnumSerializationFormat;
import org.openrdf.rio.RDFFormat;

/**
 * Various utility methods.
 * 
 * @author Jan Michelfeit
 */
public final class ODCSFusionToolUtils {
    /** Time unit 60. */
    public static final int TIME_UNIT_60 = 60;
    
    /** Number of bytes in a kilobyte. */
    public static final int KB_BYTES = 1024;
    
    /** Number of bytes in a megabyte. */
    public static final long MB_BYTES = 1024 * 1024;
    
    /** Number of bytes in a gigabyte. */
    public static final long GB_BYTES = 1024 * 1024 * 1024;
    
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
     * Creates parent directories for the given file if they don't exist already.
     * @param file file whose parent directories will be created
     * @return the given file
     */
    public static File ensureParentsExists(File file) {
        file.getAbsoluteFile().getParentFile().mkdirs();
        return file;
    }
        
    /**
     * Returns directory for cache files. Creates one if it doesn't exist yet.
     * @param tempDirectory temporary directory for execution
     * @return directory
     */
    public static File getCacheDirectory(File tempDirectory) {
        return ensureParentsExists(tempDirectory);
    }
    
    /**
     * Returns true if the given restriction is null or its pattern is empty.
     * @param restriction SPARQL restriction
     * @return true if the given restriction is null or its pattern is empty
     */
    public static boolean isRestrictionEmpty(SparqlRestriction restriction) {
        return restriction == null || ODCSUtils.isNullOrEmpty(restriction.getPattern());
    }

    /** Disable constructor for a utility class. */
    private ODCSFusionToolUtils() {
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
}
