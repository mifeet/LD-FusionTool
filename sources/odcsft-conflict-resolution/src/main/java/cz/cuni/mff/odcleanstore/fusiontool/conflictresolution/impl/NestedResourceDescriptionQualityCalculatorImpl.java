package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.CRContext;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.FQualityCalculator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.NestedResourceDescriptionQualityCalculator;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;

import java.util.Collection;

/**
 * TODO
 */
public class NestedResourceDescriptionQualityCalculatorImpl implements NestedResourceDescriptionQualityCalculator {
    private final FQualityCalculator literalNestedResourceFQualityCalculator;

    public NestedResourceDescriptionQualityCalculatorImpl(FQualityCalculator literalNestedResourceFQualityCalculator) {
        this.literalNestedResourceFQualityCalculator = literalNestedResourceFQualityCalculator;
    }

    @Override
    public double aggregateConflictClusterQuality(Collection<ResolvedStatement> conflictCluster) {
        if (conflictCluster == null || conflictCluster.isEmpty()) {
            return 0d;
        }
        double sum = 0d;
        for (ResolvedStatement resolvedStatement : conflictCluster) {
            sum += resolvedStatement.getQuality();
        }
        return sum / conflictCluster.size();
    }

    @Override
    public double getLiteralNestedResourceFQuality(Value value, Collection<Resource> sources, CRContext crContext) {
        return literalNestedResourceFQualityCalculator.getFQuality(value, crContext.getConflictingStatements(), sources, crContext);
    }
}
