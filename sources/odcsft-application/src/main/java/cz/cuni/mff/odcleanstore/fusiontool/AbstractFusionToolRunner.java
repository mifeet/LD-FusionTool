package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.util.CanonicalUriFileReader;
import cz.cuni.mff.odcleanstore.fusiontool.util.EnumFusionCounters;
import cz.cuni.mff.odcleanstore.fusiontool.util.MemoryProfiler;
import cz.cuni.mff.odcleanstore.fusiontool.util.ProfilingTimeCounter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import org.openrdf.model.Model;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Loads and prepares all inputs for data fusion executor, executes data fusion and outputs additional metadata
 * such as canonical URIs.
 * See sample configuration files (sample-config-full.xml) for overview of all processing options.
 * <p/>
 * This class is not thread-safe.
 * @see ODCSFusionToolExecutor
 */
public abstract class AbstractFusionToolRunner {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFusionToolRunner.class);

    protected boolean isProfilingOn;

    protected final CanonicalUriFileReader canonicalUriFileReader = new CanonicalUriFileReader();

    public AbstractFusionToolRunner(boolean isProfilingOn) {
        this.isProfilingOn = isProfilingOn;
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
            Model metadata = getMetadata();

            // Load & resolve owl:sameAs links
            UriMappingIterable uriMapping = getUriMapping();
            timeProfiler.stopAddCounter(EnumFusionCounters.META_INITIALIZATION);

            // Create & initialize quad loader
            timeProfiler.startCounter(EnumFusionCounters.DATA_INITIALIZATION);
            inputLoader = getInputLoader();
            inputLoader.initialize(uriMapping);
            timeProfiler.stopAddCounter(EnumFusionCounters.DATA_INITIALIZATION);

            // Initialize executor
            timeProfiler.startCounter(EnumFusionCounters.INITIALIZATION);
            ResourceDescriptionConflictResolver conflictResolver = createConflictResolver(metadata, uriMapping);
            rdfWriter = createRDFWriter();
            ODCSFusionToolExecutor executor = createExecutor(uriMapping);
            timeProfiler.stopAddCounter(EnumFusionCounters.INITIALIZATION);

            // Do the actual work
            executor.execute(inputLoader, rdfWriter, conflictResolver);

            // Write metadata
            timeProfiler.startCounter(EnumFusionCounters.META_OUTPUT_WRITING);
            writeCanonicalURIs(uriMapping);
            writeSameAsLinks(uriMapping);
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

    protected abstract InputLoader getInputLoader() throws IOException, ODCSFusionToolException;

    protected abstract CloseableRDFWriter createRDFWriter() throws IOException, ODCSFusionToolException;

    protected abstract Model getMetadata() throws ODCSFusionToolException;

    protected abstract UriMappingIterable getUriMapping() throws ODCSFusionToolException, IOException;

    protected abstract ResourceDescriptionConflictResolver createConflictResolver(Model metadata, UriMappingIterable uriMapping);

    protected abstract ODCSFusionToolExecutor createExecutor(UriMappingIterable uriMapping);

    protected abstract void writeCanonicalURIs(UriMappingIterable uriMapping) throws IOException;

    protected abstract void writeSameAsLinks(UriMappingIterable uriMapping) throws IOException, ODCSFusionToolException;

    /**
     * Returns set of URIs preferred for canonical URIs.
     * The URIs are loaded from canonicalURIsInputFile if given and URIs present in settingsPreferredURIs are added.
     * @param settingsPreferredURIs URIs occurring on fusion tool configuration
     * @param canonicalURIsInputFile file with canonical URIs to be loaded; can be null
     * @param preferredCanonicalURIs default set of preferred canonical URIs
     * @return set of URIs preferred for canonical URIs
     * @throws java.io.IOException error reading canonical URIs from file
     */
    protected Set<String> getPreferredURIs(
            Set<URI> settingsPreferredURIs, File canonicalURIsInputFile,
            Collection<String> preferredCanonicalURIs) throws IOException {

        Set<String> preferredURIs = new HashSet<>(settingsPreferredURIs.size());
        for (URI uri : settingsPreferredURIs) {
            preferredURIs.add(uri.stringValue());
        }
        if (canonicalURIsInputFile != null) {
            canonicalUriFileReader.readCanonicalUris(canonicalURIsInputFile, preferredURIs);
        }
        preferredURIs.addAll(preferredCanonicalURIs);

        return preferredURIs;
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
