/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Container of settings for an output of result data.
 * @author Jan Michelfeit
 */
public abstract class SourceConfigImpl implements SourceConfig {
    private final EnumDataSourceType type;
    private final String name;
    private final Map<String, String> params = new HashMap<String, String>();
    
    /**
     * @param type data source type
     * @param name human-readable name or null
     */
    public SourceConfigImpl(EnumDataSourceType type, String name) {
        this.type = type;
        this.name = name;
    }

    @Override
    public EnumDataSourceType getType() {
        return type;
    }
    
    @Override
    public String getName() {
        return name; 
    }
    
    @Override
    public Map<String, String> getParams() {
        return params;
    }

    @Override
    public String toString() {
        return name != null ? name : type.toString();
    }
}
