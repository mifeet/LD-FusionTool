/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Root;

/**
 * @author Jan Michelfeit
 */
@Root(name = "Property")
public class PropertyXml {
    @Attribute
    private String id;
    
    public String getId() {
        return id;
    }
}
