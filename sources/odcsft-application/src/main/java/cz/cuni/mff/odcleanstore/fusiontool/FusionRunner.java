package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.util.EnumFusionCounters;
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
 * @see LDFusionToolExecutor
 */
public class FusionRunner {
    protected boolean isProfilingOn = false;
    protected final FusionComponentFactory componentFactory;
    private ProfilingTimeCounter<EnumFusionCounters> timeProfiler;

    public FusionRunner(FusionComponentFactory componentFactory) {
        this.componentFactory = componentFactory;
    }

    public void setProfilingOn(boolean isProfilingOn) {
        this.isProfilingOn = isProfilingOn;
    }

    /**
     * Returns profiling information about the last execution of {@link #runFusionTool()}.
     * The profiling information will contain meaningful values only if profiling is turned on with {@link #setProfilingOn(boolean)}.
     * @return profiling info
     */
    public ProfilingTimeCounter<EnumFusionCounters> getTimeProfiler() {
        return timeProfiler;
    }

    /**
     * Performs the actual LD-FusionTool task according to the given configuration.
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException general fusion error
     * @throws java.io.IOException I/O error when writing results
     * @throws cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException conflict resolution error
     */
    public void runFusionTool() throws LDFusionToolException, IOException, ConflictResolutionException {
        InputLoader inputLoader = null;
        CloseableRDFWriter rdfWriter = null;
        timeProfiler = ProfilingTimeCounter.createInstance(EnumFusionCounters.class, isProfilingOn);
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
            FusionExecutor executor = componentFactory.getExecutor(uriMapping);
            timeProfiler.stopAddCounter(EnumFusionCounters.INITIALIZATION);

            // Do the actual work
            executor.fuse(conflictResolver, inputLoader, rdfWriter);

            // Write metadata
            timeProfiler.startCounter(EnumFusionCounters.META_OUTPUT_WRITING);
            componentFactory.getCanonicalUriWriter(uriMapping).write(uriMapping);
            componentFactory.getSameAsLinksWriter().write(uriMapping);
            timeProfiler.stopAddCounter(EnumFusionCounters.META_OUTPUT_WRITING);
        } finally {
            if (rdfWriter != null) {
                rdfWriter.close();
            }
            if (inputLoader != null) {
                inputLoader.close();
            }
        }
    }

}
