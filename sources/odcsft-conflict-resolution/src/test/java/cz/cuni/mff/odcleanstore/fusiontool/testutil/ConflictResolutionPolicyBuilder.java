package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolutionPolicy;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictResolutionPolicyImpl;
import org.openrdf.model.URI;

import java.util.HashMap;

public class ConflictResolutionPolicyBuilder {
    private final ConflictResolutionPolicyImpl conflictResolutionPolicy;

    public static ConflictResolutionPolicyBuilder newPolicy() {
        return new ConflictResolutionPolicyBuilder();
    }

    public ConflictResolutionPolicyBuilder() {
        this.conflictResolutionPolicy = new ConflictResolutionPolicyImpl();
        this.conflictResolutionPolicy.setPropertyResolutionStrategy(new HashMap<URI, ResolutionStrategy>());
    }

    public ConflictResolutionPolicyBuilder with(URI uri, ResolutionStrategy resolutionStrategy) {
        conflictResolutionPolicy.getPropertyResolutionStrategies().put(uri, resolutionStrategy);
        return this;
    }

    public ConflictResolutionPolicy build() {
        return conflictResolutionPolicy;
    }
}
