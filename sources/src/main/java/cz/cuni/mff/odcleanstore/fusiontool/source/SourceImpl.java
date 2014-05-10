/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.source;

import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import org.openrdf.repository.Repository;

import java.util.Map;

/**
 * Container for RDF {@link Repository} and related settings.
 * @author Jan Michelfeit
 */
public abstract class SourceImpl implements Source {
    private final Repository repository;
    private final Map<String, String> prefixes;
    private final String label;
    private final EnumDataSourceType type;

    /**
     * Creates a new instance.
     * @param repository repository providing access to actual data
     * @param prefixes map of namespace prefixes
     * @param label name of this data source
     * @param type type of this data source
     */
    public SourceImpl(Repository repository, Map<String, String> prefixes,
            String label, EnumDataSourceType type) {
        this.repository = repository;
        this.prefixes = prefixes;
        this.label = label;
        this.type = type;
    }

    @Override
    public Repository getRepository() {
        return repository;
    }

    @Override
    public Map<String, String> getPrefixes() {
        return prefixes;
    }

    @Override
    public String getName() {
        return label;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public EnumDataSourceType getType() {
        return type;
    }
}
