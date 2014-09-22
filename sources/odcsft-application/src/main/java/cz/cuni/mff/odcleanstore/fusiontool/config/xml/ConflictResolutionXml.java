/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config.xml;

import java.util.List;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "ConflictResolution")
public class ConflictResolutionXml {
    @Element(name = "DefaultStrategy", required = false)
    private ResolutionStrategyXml defaultResolutionStrategy;
    
    @ElementList(name = "ResolutionStrategy", required = false, inline = true, empty = false)
    private List<PropertyResolutionStrategyXml> propertyResolutionStrategies;

    public ResolutionStrategyXml getDefaultResolutionStrategy() {
        return defaultResolutionStrategy;
    }

    public List<PropertyResolutionStrategyXml> getPropertyResolutionStrategies() {
        return propertyResolutionStrategies;
    }


}
