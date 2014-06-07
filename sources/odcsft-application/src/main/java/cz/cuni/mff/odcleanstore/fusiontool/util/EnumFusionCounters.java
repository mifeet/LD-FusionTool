/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.util;

/**
 * Types of time counters available in profiling mode. 
 * @author Jan Michelfeit
 */
public enum EnumFusionCounters {
    /** Length of initialization (loading and resolution of sameAs links, metadata etc.). */
    INITIALIZATION,
    
    /** Query time spent loading quads. */
    QUAD_LOADING,
    
    /** Time spent by conflict resolution. */
    CONFLICT_RESOLUTION,
    
    /** Time spent by writing to outputs. */
    OUTPUT_WRITING,

    /** Time spent by accessing the buffer of URIs to be processed etc. (useful if the buffer is stored on disk). */
    BUFFERING,

    /** Time spent writing metadata, canonical URIs or owl:sameAs links. */
    META_OUTPUT_WRITING,

    /** Time spent initializing data source */
    DATA_INITIALIZATION,

    /** Time reading metadata and owl:sameAs links. */
    META_INITIALIZATION,

    /** Time spent filtering input data. */
    INPUT_FILTERING
}
