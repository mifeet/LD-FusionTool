package cz.cuni.mff.odcleanstore.crbatch.util;

import java.io.File;

import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import cz.cuni.mff.odcleanstore.shared.ODCSUtils;

/**
 * Various utility methods.
 * 
 * @author Jan Michelfeit
 */
public final class CRBatchUtils {
    /** Time unit 60. */
    public static final int TIME_UNIT_60 = 60;
    
    /** Number of bytes in a kilobyte. */
    public static final int KB_BYTES = 1024;
    
    /** Number of bytes in a megabyte. */
    public static final long MB_BYTES = 1024 * 1024;
    
    /** Number of bytes in a gigabyte. */
    public static final long GB_BYTES = 1024 * 1024 * 1024;

    /**
     * Returns an URI representing the given node or null if it is not a resource.
     * For blank nodes returns the Virtuoso blank node identifier.
     * @param value RDF node
     * @return URI representing
     */
    public static String getNodeURI(Value value) {
        if (value instanceof URI) {
            return value.stringValue();
        } else if (value instanceof BNode) {
            return ODCSUtils.getVirtuosoURIForBlankNode((BNode) value);
        } else {
            return null;
        }
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
     * Creates parent directories for the given file if they don't exist already.
     * @param file file whose parent directories will be created
     */
    public static void ensureParentsExists(File file) {
        file.getAbsoluteFile().getParentFile().mkdirs();
    }

    /** Disable constructor for a utility class. */
    private CRBatchUtils() {
    }
}
