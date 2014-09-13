package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.util.EnumFusionCounters;
import cz.cuni.mff.odcleanstore.fusiontool.util.MemoryProfiler;
import cz.cuni.mff.odcleanstore.fusiontool.util.ProfilingTimeCounter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.UriMappingWriter;
import org.junit.Test;
import org.openrdf.model.Model;

import static org.mockito.Mockito.*;

public class FusionToolRunnerTest {
    @Test
    public void runsFusionTool() throws Exception {
        FusionToolComponentFactory componentFactory = mock(FusionToolComponentFactory.class);
        UriMappingIterable uriMapping = mock(UriMappingIterable.class);
        when(componentFactory.getUriMapping()).thenReturn(uriMapping);

        Model metadata = mock(Model.class);
        when(componentFactory.getMetadata()).thenReturn(metadata);

        InputLoader inputLoader = mock(InputLoader.class);
        when(componentFactory.getInputLoader()).thenReturn(inputLoader);

        ResourceDescriptionConflictResolver conflictResolver = mock(ResourceDescriptionConflictResolver.class);
        when(componentFactory.getConflictResolver(metadata, uriMapping)).thenReturn(conflictResolver);

        CloseableRDFWriter rdfWriter = mock(CloseableRDFWriter.class);
        when(componentFactory.getRDFWriter()).thenReturn(rdfWriter);

        FusionToolExecutor executor = mock(FusionToolExecutor.class);
        when(executor.getMemoryProfiler()).thenReturn(MemoryProfiler.createInstance(true));
        when(executor.getTimeProfiler()).thenReturn(ProfilingTimeCounter.createInstance(EnumFusionCounters.class, true));
        when(componentFactory.getExecutor(uriMapping)).thenReturn(executor);

        UriMappingWriter canonicalUriWriter = mock(UriMappingWriter.class);
        when(componentFactory.getCanonicalUriWriter(uriMapping)).thenReturn(canonicalUriWriter);

        UriMappingWriter sameAsWriter = mock(UriMappingWriter.class);
        when(componentFactory.getSameAsLinksWriter()).thenReturn(sameAsWriter);

        FusionToolRunner runner = new FusionToolRunner(componentFactory);
        runner.setProfilingOn(true);
        runner.runFusionTool();

        verify(inputLoader).initialize(uriMapping);
        verify(executor).fuse(conflictResolver, inputLoader, rdfWriter);
        verify(canonicalUriWriter).write(uriMapping);
        verify(sameAsWriter).write(uriMapping);
    }
}