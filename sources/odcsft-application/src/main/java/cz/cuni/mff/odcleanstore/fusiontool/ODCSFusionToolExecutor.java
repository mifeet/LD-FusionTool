package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.util.EnumFusionCounters;
import cz.cuni.mff.odcleanstore.fusiontool.util.MemoryProfiler;
import cz.cuni.mff.odcleanstore.fusiontool.util.ProfilingTimeCounter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * Fuses RDF data loaded from RDF sources using ODCS Conflict Resolution and writes the output to RDF outputs.
 * Conflict resolution includes resolution of owl:sameAs link, resolution of instance-level conflicts.
 * See sample configuration files (sample-config-full.xml) for overview of all processing options.
 *
 * This class is not thread-safe.
*/
public class ODCSFusionToolExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ODCSFusionToolExecutor.class);

    private final boolean hasVirtuosoSource;
    private final Long maxOutputTriples;
    private final ProfilingTimeCounter<EnumFusionCounters> timeProfiler;
    private final MemoryProfiler memoryProfiler;

    public ODCSFusionToolExecutor() {
        this(false, null, false);
    }

    /**
     * @param hasVirtuosoSource indicates whether the {@link cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader}
     *      given to {@code execute()} may contain a source of type
     *      {@link cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType#VIRTUOSO} (need for Virtuoso bug circumvention).
     * @param maxOutputTriples maximum number of triples to be processed; null means unlimited
     * @param isProfilingOn whether to measure profiling information
     */
    public ODCSFusionToolExecutor(boolean hasVirtuosoSource, Long maxOutputTriples, boolean isProfilingOn) {
        this.hasVirtuosoSource = hasVirtuosoSource;
        this.maxOutputTriples = maxOutputTriples;
        timeProfiler = ProfilingTimeCounter.createInstance(EnumFusionCounters.class, isProfilingOn);
        memoryProfiler = MemoryProfiler.createInstance(isProfilingOn);
    }

    public ProfilingTimeCounter<EnumFusionCounters> getTimeProfiler() {
        return timeProfiler;
    }

    public MemoryProfiler getMemoryProfiler() {
        return memoryProfiler;
    }

    public void execute(InputLoader inputLoader, CloseableRDFWriter rdfWriter, ConflictResolver conflictResolver)
            throws ODCSFusionToolException, ConflictResolutionException, IOException {

        // Initialize triple counters
        long outputTriples = 0;
        long inputTriples = 0;
        boolean checkMaxOutputTriples = maxOutputTriples != null && maxOutputTriples >= 0;

        // Load & process input quads
        timeProfiler.startCounter(EnumFusionCounters.BUFFERING);
        while (inputLoader.hasNext()) {
            timeProfiler.stopAddCounter(EnumFusionCounters.BUFFERING);

            // Load quads for the given subject
            timeProfiler.startCounter(EnumFusionCounters.QUAD_LOADING);
            ResourceDescription resourceDescription = inputLoader.next();
            inputTriples += resourceDescription.getDescribingStatements().size();
            timeProfiler.stopAddCounter(EnumFusionCounters.QUAD_LOADING);

            // Resolve conflicts
            timeProfiler.startCounter(EnumFusionCounters.CONFLICT_RESOLUTION);
            Collection<ResolvedStatement> resolvedQuads = conflictResolver.resolveConflicts(resourceDescription.getDescribingStatements());
            timeProfiler.stopAddCounter(EnumFusionCounters.CONFLICT_RESOLUTION);
            LOG.info("Resolved {} quads resulting in {} quads (processed totally {} quads)",
                    new Object[] {resourceDescription.getDescribingStatements().size(), resolvedQuads.size(), inputTriples});

            // Check if we have reached the limit on output triples
            if (checkMaxOutputTriples && outputTriples + resolvedQuads.size() > maxOutputTriples) {
                break;
            }
            outputTriples += resolvedQuads.size();

            // Add objects filtered by CR for traversal
            timeProfiler.startCounter(EnumFusionCounters.BUFFERING);
            inputLoader.updateWithResolvedStatements(resolvedQuads);
            timeProfiler.stopAddCounter(EnumFusionCounters.BUFFERING);

            // Write result to output
            timeProfiler.startCounter(EnumFusionCounters.OUTPUT_WRITING);
            rdfWriter.writeResolvedStatements(resolvedQuads);
            timeProfiler.stopAddCounter(EnumFusionCounters.OUTPUT_WRITING);

            memoryProfiler.capture();
            fixVirtuosoOpenedStatements();
            timeProfiler.startCounter(EnumFusionCounters.BUFFERING);
        }
        timeProfiler.stopAddCounter(EnumFusionCounters.BUFFERING);
        LOG.info(String.format("Processed %,d quads which were resolved to %,d output quads.", inputTriples, outputTriples));
    }

    /**
     * Fixes bug in Virtuoso which doesn't release connections even when they are released explicitly.
     * This method puts the current thread to sleep so that the thread releasing connections has chance to be
     * planned for execution. If hasVirtuosoSource is false, does nothing.
     */
    protected void fixVirtuosoOpenedStatements() {
        if (hasVirtuosoSource) {
            // Somehow helps Virtuoso release connections. Without call to Thread.sleep(),
            // application may fail with "No buffer space available (maximum connections reached?)"
            // exception for too many named graphs.
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}
