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
        maxTotalMemory = Math.max(maxTotalMemory, Runtime.getRuntime().totalMemory());
    }
    
    /**
     * Returns maximum measured total memory in bytes.
     * @return maximum measured total memory in bytes
     */
    public long getMaxTotalMemory() {
        return maxTotalMemory;
    }
    
    /**
     * Returns maximum measured total memory as a human-readable string.
     * @return formatted maximum total memory
     */
    public String formatMaxTotalMemory() {
        return String.format(Locale.ROOT,  "%,.1f MB", maxTotalMemory / (double) MB_BYTES);
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
