package cz.cuni.mff.odcleanstore.fusiontool.config;

import java.io.File;

/**
 * Configuration related to data processing.
 */
interface ConfigProcessing {
    /**
     * Indicates whether disable (true) or enable (false) file cache for objects that needed by CR algorithm
     * that may not fit into the memory.
     * @return whether to disable algorithm file cache
     */
    boolean getEnableFileCache(); // TODO: detect automatically whether it's neccessary

    /**
     * Directory for temporary files.
     * @return directory for temporary files.
     */
    File getTempDirectory();

    /**
     * Indicates whether data should be downloaded to a local file prior to processing.
     * @return true iff data should be pre-downloaded to a local file
     */
    boolean isLocalCopyProcessing();

    /**
     * Maximum memory amount to use for large operations.
     * Null means no limit.
     * If the limit is too high, it may cause OutOfMemory exceptions.
     * @return memory limit in bytes or null for no limit
     */
    Long getMemoryLimit();

    /**
     * Max portion of free memory to use.
     * @return portion of free memory to use as a number between 0 and 1
     */
    float getMaxFreeMemoryUsage();
}
