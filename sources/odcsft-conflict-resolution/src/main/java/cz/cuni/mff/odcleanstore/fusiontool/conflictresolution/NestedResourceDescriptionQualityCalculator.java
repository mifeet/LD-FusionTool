package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution;

import cz.cuni.mff.odcleanstore.conflictresolution.CRContext;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;

import java.util.Collection;

/**
 * TODO
 */
public interface NestedResourceDescriptionQualityCalculator {
    double aggregateConflictClusterQuality(Collection<ResolvedStatement> conflictCluster);

    double getLiteralNestedResourceFQuality(Value value, Collection<Resource> sources, CRContext crContext);
}
