/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "Param")
public class ParamXml {
    @Attribute
    private String name;

    @Attribute
    private String value;

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }
}
