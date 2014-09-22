/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config.xml;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "Prefix")
public class PrefixXml {
    @Attribute
    private String id;

    @Attribute
    private String namespace;

    public String getId() {
        return id;
    }

    public String getNamespace() {
        return namespace;
    }
}
