package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.util.EnumFusionCounters;
import cz.cuni.mff.odcleanstore.fusiontool.util.MemoryProfiler;
import cz.cuni.mff.odcleanstore.fusiontool.util.ProfilingTimeCounter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;

import java.io.IOException;

/**
 * Fuses RDF data loaded from RDF sources using ODCS Conflict Resolution and writes the output to RDF outputs.
 * Fusion includes resolution of owl:sameAs link, resolution of instance-level conflicts.
 */
public interface FusionToolExecutor {
    void fuse(ResourceDescriptionConflictResolver conflictResolver, InputLoader inputLoader, CloseableRDFWriter rdfWriter)
            throws ODCSFusionToolException, ConflictResolutionException, IOException;

    MemoryProfiler getMemoryProfiler();

    ProfilingTimeCounter<EnumFusionCounters> getTimeProfiler();
}
