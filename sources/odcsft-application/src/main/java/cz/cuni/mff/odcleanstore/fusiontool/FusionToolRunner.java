package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.util.EnumFusionCounters;
import cz.cuni.mff.odcleanstore.fusiontool.util.MemoryProfiler;
import cz.cuni.mff.odcleanstore.fusiontool.util.ProfilingTimeCounter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import org.openrdf.model.Model;

import java.io.IOException;

/**
 * Loads and prepares all inputs for data fusion executor, executes data fusion and outputs additional metadata
 * such as canonical URIs.
 * See sample configuration files (sample-config-full.xml) for overview of all processing options.
 * <p/>
 * This class is not thread-safe.
 * @see ODCSFusionToolExecutor
 */
public class FusionToolRunner {
    protected final boolean isProfilingOn;
    protected final FusionToolComponentFactory componentFactory;

    public FusionToolRunner(FusionToolComponentFactory componentFactory, boolean isProfilingOn) {
        this.isProfilingOn = isProfilingOn;
        this.componentFactory = componentFactory;
    }

    /**
     * Performs the actual LD-FusionTool task according to the given configuration.
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException general fusion error
     * @throws java.io.IOException I/O error when writing results
     * @throws cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException conflict resolution error
     */
    public void runFusionTool() throws ODCSFusionToolException, IOException, ConflictResolutionException {
        InputLoader inputLoader = null;
        CloseableRDFWriter rdfWriter = null;
        ProfilingTimeCounter<EnumFusionCounters> timeProfiler = ProfilingTimeCounter.createInstance(EnumFusionCounters.class, isProfilingOn);
        try {
            // Load source named graphs metadata
            timeProfiler.startCounter(EnumFusionCounters.META_INITIALIZATION);
            Model metadata = componentFactory.getMetadata();

            // Load & resolve owl:sameAs links
            UriMappingIterable uriMapping = componentFactory.getUriMapping();
            timeProfiler.stopAddCounter(EnumFusionCounters.META_INITIALIZATION);

            // Create & initialize quad loader
            timeProfiler.startCounter(EnumFusionCounters.DATA_INITIALIZATION);
            inputLoader = componentFactory.getInputLoader();
            inputLoader.initialize(uriMapping);
            timeProfiler.stopAddCounter(EnumFusionCounters.DATA_INITIALIZATION);

            // Initialize executor
            timeProfiler.startCounter(EnumFusionCounters.INITIALIZATION);
            ResourceDescriptionConflictResolver conflictResolver = componentFactory.getConflictResolver(metadata, uriMapping);
            rdfWriter = componentFactory.getRDFWriter();
            FusionToolExecutor executor = componentFactory.getExecutor(uriMapping);
            timeProfiler.stopAddCounter(EnumFusionCounters.INITIALIZATION);

            // Do the actual work
            executor.fuse(conflictResolver, inputLoader, rdfWriter);

            // Write metadata
            timeProfiler.startCounter(EnumFusionCounters.META_OUTPUT_WRITING);
            componentFactory.getCanonicalUriWriter(uriMapping).write(uriMapping);
            componentFactory.getSameAsLinksWriter().write(uriMapping);
            timeProfiler.stopAddCounter(EnumFusionCounters.META_OUTPUT_WRITING);

            // Print profiling information
            timeProfiler.addProfilingTimeCounter(executor.getTimeProfiler());
            printProfilingInformation(timeProfiler, executor.getMemoryProfiler());
        } finally {
            if (rdfWriter != null) {
                rdfWriter.close();
            }
            if (inputLoader != null) {
                inputLoader.close();
            }
        }
    }

    /**
     * Prints profiling information from the given profiling time counter.
     * @param timeProfiler profiling time counter
     * @param memoryProfiler memory profiler
     */
    protected void printProfilingInformation(
            ProfilingTimeCounter<EnumFusionCounters> timeProfiler,
            MemoryProfiler memoryProfiler) {

        if (isProfilingOn) {
            System.out.println("-- Profiling information --------");
            System.out.println("Initialization time:              " + timeProfiler.formatCounter(EnumFusionCounters.INITIALIZATION));
            System.out.println("Reading metadata & sameAs links:  " + timeProfiler.formatCounter(EnumFusionCounters.META_INITIALIZATION));
            System.out.println("Data sources initialization time: " + timeProfiler.formatCounter(EnumFusionCounters.DATA_INITIALIZATION));
            System.out.println("Quad loading time:                " + timeProfiler.formatCounter(EnumFusionCounters.QUAD_LOADING));
            System.out.println("Input filtering time:             " + timeProfiler.formatCounter(EnumFusionCounters.INPUT_FILTERING));
            System.out.println("Buffering time:                   " + timeProfiler.formatCounter(EnumFusionCounters.BUFFERING));
            System.out.println("Conflict resolution time:         " + timeProfiler.formatCounter(EnumFusionCounters.CONFLICT_RESOLUTION));
            System.out.println("Output writing time:              " + timeProfiler.formatCounter(EnumFusionCounters.OUTPUT_WRITING));
            System.out.println("Maximum recorded total memory:    " + MemoryProfiler.formatMemoryBytes(memoryProfiler.getMaxTotalMemory()));
            System.out.println("Maximum recorded used memory:     " + MemoryProfiler.formatMemoryBytes(memoryProfiler.getMaxUsedMemory()));
            System.out.println("Minimum recorded free memory:     " + MemoryProfiler.formatMemoryBytes(memoryProfiler.getMinFreeMemory()));
        }
    }

}
