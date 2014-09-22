package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter.ResourceDescriptionFilter;
import cz.cuni.mff.odcleanstore.fusiontool.util.EnumFusionCounters;
import cz.cuni.mff.odcleanstore.fusiontool.util.IsCanceledCallback;
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
public class LDFusionToolExecutor implements FusionExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(LDFusionToolExecutor.class);

    private final boolean hasVirtuosoSource;
    private final Long maxOutputTriples;
    private final ProfilingTimeCounter<EnumFusionCounters> timeProfiler;
    private final MemoryProfiler memoryProfiler;
    private ResourceDescriptionFilter resourceDescriptionFilter;
    private IsCanceledCallback isCanceledCallback;

    /**
     * @param hasVirtuosoSource indicates whether the {@link cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader}
     *      given to {@code fuse()} may contain a source of type
     *      {@link cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType#VIRTUOSO} (need for Virtuoso bug circumvention).
     * @param maxOutputTriples maximum number of triples to be processed; null means unlimited
     * @param timeProfiler time profiler
     * @param memoryProfiler memory profiler
     */
    public LDFusionToolExecutor(
            boolean hasVirtuosoSource,
            Long maxOutputTriples,
            ResourceDescriptionFilter resourceDescriptionFilter,
            ProfilingTimeCounter<EnumFusionCounters> timeProfiler,
            MemoryProfiler memoryProfiler) {
        this.hasVirtuosoSource = hasVirtuosoSource;
        this.maxOutputTriples = maxOutputTriples;
        this.resourceDescriptionFilter = resourceDescriptionFilter;
        this.timeProfiler = timeProfiler;
        this.memoryProfiler = memoryProfiler;
    }

    @Override
    public void fuse(ResourceDescriptionConflictResolver conflictResolver, InputLoader inputLoader, CloseableRDFWriter rdfWriter)
            throws LDFusionToolException, ConflictResolutionException, IOException {

        // Initialize triple counters
        long outputTriples = 0;
        long inputTriples = 0;
        boolean checkMaxOutputTriples = maxOutputTriples != null && maxOutputTriples >= 0;

        // Load & process input quads
        LOG.info("Starting conflict resolution");
        timeProfiler.startCounter(EnumFusionCounters.BUFFERING);
        while (!isCanceled() && inputLoader.hasNext()) {
            timeProfiler.stopAddCounter(EnumFusionCounters.BUFFERING);

            // Load quads for the given subject
            timeProfiler.startCounter(EnumFusionCounters.QUAD_LOADING);
            ResourceDescription resourceDescription = inputLoader.next();
            inputTriples += resourceDescription.getDescribingStatements().size();
            timeProfiler.stopAddCounter(EnumFusionCounters.QUAD_LOADING);

            // Apply input filters
            timeProfiler.startCounter(EnumFusionCounters.INPUT_FILTERING);
            boolean accept = this.resourceDescriptionFilter.accept(resourceDescription);
            timeProfiler.stopAddCounter(EnumFusionCounters.INPUT_FILTERING);
            if (!accept) {
                LOG.debug("Resource {} doesn't match filter, skipping", resourceDescription.getResource());
                timeProfiler.startCounter(EnumFusionCounters.BUFFERING);
                continue;
            }

            // Resolve conflicts
            timeProfiler.startCounter(EnumFusionCounters.CONFLICT_RESOLUTION);
            Collection<ResolvedStatement> resolvedQuads = conflictResolver.resolveConflicts(resourceDescription);
            timeProfiler.stopAddCounter(EnumFusionCounters.CONFLICT_RESOLUTION);
            LOG.debug("Resolved {} quads resulting in {} quads (processed totally {} quads)",
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
        if (isCanceled()) {
            LOG.warn("The execution was canceled!");
        }
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

    public Long getMaxOutputTriples() {
        return maxOutputTriples;
    }

    public ResourceDescriptionFilter getResourceDescriptionFilter() {
        return resourceDescriptionFilter;
    }

    public void setIsCanceledCallback(IsCanceledCallback isCanceledCallback) {
        this.isCanceledCallback = isCanceledCallback;
    }

    private boolean isCanceled() {
        return isCanceledCallback != null && isCanceledCallback.isCanceled();
    }
}

