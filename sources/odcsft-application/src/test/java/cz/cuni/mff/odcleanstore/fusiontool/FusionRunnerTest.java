package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.UriMappingWriter;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Model;

import static org.mockito.Mockito.*;

public class FusionRunnerTest {

    private FusionComponentFactory componentFactory;
    private UriMappingIterable uriMapping;
    private Model metadata;
    private InputLoader inputLoader;
    private ResourceDescriptionConflictResolver conflictResolver;
    private CloseableRDFWriter rdfWriter;
    private FusionExecutor executor;
    private UriMappingWriter canonicalUriWriter;
    private UriMappingWriter sameAsWriter;

    @Before
    public void setUp() throws Exception {
        componentFactory = mock(FusionComponentFactory.class);
        uriMapping = mock(UriMappingIterable.class);
        when(componentFactory.getUriMapping()).thenReturn(uriMapping);

        metadata = mock(Model.class);
        when(componentFactory.getMetadata()).thenReturn(metadata);

        inputLoader = mock(InputLoader.class);
        when(componentFactory.getInputLoader()).thenReturn(inputLoader);

        conflictResolver = mock(ResourceDescriptionConflictResolver.class);
        when(componentFactory.getConflictResolver(metadata, uriMapping)).thenReturn(conflictResolver);

        rdfWriter = mock(CloseableRDFWriter.class);
        when(componentFactory.getRDFWriter()).thenReturn(rdfWriter);

        executor = mock(FusionExecutor.class);
        when(componentFactory.getExecutor(uriMapping)).thenReturn(executor);

        canonicalUriWriter = mock(UriMappingWriter.class);
        when(componentFactory.getCanonicalUriWriter(uriMapping)).thenReturn(canonicalUriWriter);

        sameAsWriter = mock(UriMappingWriter.class);
        when(componentFactory.getSameAsLinksWriter()).thenReturn(sameAsWriter);
    }

    @Test
    public void runsFusionTool() throws Exception {
        FusionRunner runner = new FusionRunner(componentFactory);
        runner.setProfilingOn(true);
        runner.runFusionTool();

        verify(inputLoader).initialize(uriMapping);
        verify(executor).fuse(conflictResolver, inputLoader, rdfWriter);
        verify(canonicalUriWriter).write(uriMapping);
        verify(sameAsWriter).write(uriMapping);
    }

}