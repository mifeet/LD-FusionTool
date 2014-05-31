package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final long HOUR_MS = ODCSUtils.MILLISECONDS * ODCSUtils.TIME_UNIT_60 * ODCSUtils.TIME_UNIT_60;

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

    /** Disable constructor for a utility class. */
    private ODCSFusionToolAppUtils() {
    }
}
