/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.util;

/**
 * Types of time counters available in profiling mode. 
 * @author Jan Michelfeit
 */
public enum ProfilingCounters {
    /** Length of initialization (loading and resolution of sameAs links, metadata etc.). */
    INITIALIZATION,
    
    /** Query time spent loading quads. */
    QUAD_LOADING,
    
    /** Time spent by conflict resolution. */
    CONFLICT_RESOLUTION,
    
    /** Time spent by writing to outputs. */
    OUTPUT_WRITING,
    
    /** Time spent by accessing the buffer of URIs to be processed etc. (useful if the buffer is stored on disk). */
    BUFFERING
}
