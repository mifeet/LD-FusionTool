/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config.xml;

import java.util.List;

import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "ResolutionStrategy")
public class PropertyResolutionStrategyXml extends ResolutionStrategyXml {
    @ElementList(required = false, empty = false, inline = true)
    private List<PropertyXml> properties;

    public List<PropertyXml> getProperties() {
        return properties;
    }

}