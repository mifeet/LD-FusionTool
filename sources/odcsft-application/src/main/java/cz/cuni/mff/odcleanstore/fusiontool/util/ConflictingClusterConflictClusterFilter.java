package cz.cuni.mff.odcleanstore.fusiontool.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import cz.cuni.mff.odcleanstore.conflictresolution.CRContext;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictClusterFilter;
import cz.cuni.mff.odcleanstore.conflictresolution.resolution.utils.ObjectClusterIterator;

/**
 * A conflict cluster filter that filters out all conflict clusters that do not contain any conflict.
 * @author Jan Michelfeit
 */
public class ConflictingClusterConflictClusterFilter implements ConflictClusterFilter {
    
    @Override
    public List<Statement> filter(List<Statement> conflictCluster, CRContext context) {
        Set<Resource> expectedGraphs = null;
        ObjectClusterIterator objectClusterIt = new ObjectClusterIterator(conflictCluster);
        while (objectClusterIt.hasNext()) {
            objectClusterIt.next();
            Collection<Resource> objectClusterGraphs = objectClusterIt.peekSources();

            if (expectedGraphs == null) {
                expectedGraphs = new HashSet<Resource>(objectClusterGraphs);
            } else if (!expectedGraphs.containsAll(objectClusterGraphs) || expectedGraphs.size() != objectClusterGraphs.size()) {
                // There is an object that is not in all the graphs iff values claimed by all graphs are not the same.
                return conflictCluster;
            }
        }
        return Collections.emptyList();
    }
}
