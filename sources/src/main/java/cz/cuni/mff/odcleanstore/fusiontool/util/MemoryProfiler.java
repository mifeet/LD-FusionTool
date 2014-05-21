/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.util;

import java.util.Locale;

/**
 * Class providing methods for profiling of memory.
 * @author Jan Michelfeit
 */
// CHECKSTYLE:OFF
public class MemoryProfiler {
// CHECKSTYLE:ON
    private static final long MB_BYTES = 1024 * 1024;
    
    private long maxTotalMemory;
    private long maxUsedMemory;
    private long minFreeMemory;

    /** 
     * Returns a new instance with profiling enabled or disabled according to profilingOn parameter.
     * @param isProfilingOn whether memory profiling are enabled or disabled; if disabled, no measurements are performed
     * @return a new instance of {@link MemoryProfiler}
     */
    public static MemoryProfiler createInstance(boolean isProfilingOn) {
        if (isProfilingOn) {
            return new MemoryProfiler();
        } else {
            return new DummyMemoryProfiler();
        }
    }
    
    private MemoryProfiler() {
    }

    /** Measures the current total consumed memory.  */
    public void capture() {
        Runtime runtime = Runtime.getRuntime();
        maxTotalMemory = Math.max(maxTotalMemory, runtime.totalMemory());
        maxUsedMemory = Math.max(maxUsedMemory, runtime.totalMemory() - runtime.freeMemory());
        minFreeMemory = Math.min(minFreeMemory, runtime.freeMemory());
    }

    /**
     * Returns the given size in Bytes as a human-readable string.
     * @return formatted memory amount
     */
    public static String formatMemoryBytes(long bytes) {
        return String.format(Locale.ROOT,  "%,.1f MB", bytes / (double) MB_BYTES);
    }

    /**
     * Returns maximum measured total memory in bytes.
     * @return maximum measured total memory in bytes
     */
    public long getMaxTotalMemory() {
        return maxTotalMemory;
    }

    /**
     * Returns minimum measured free memory in bytes.
     * @return minimum measured free memory in bytes
     */
    public long getMinFreeMemory() {
        return minFreeMemory;
    }

    /**
     * Returns maximum measured used memory in bytes.
     * @return maximum measured used memory in bytes
     */
    public long getMaxUsedMemory() {
        return maxUsedMemory;
    }

    /**
     * Child class which doesn't perform any measurements for use when profiling is turned off.
     */
    private static class DummyMemoryProfiler extends MemoryProfiler {
        @Override
        public void capture() {
            // do nothing 
        }
    }
    
}
