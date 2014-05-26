/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config.xml;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

@Root
public class SeedResourceRestrictionXml extends RestrictionXml {
    @Attribute(required = false)
    private String transitive;

    public String getTransitive() {
        return transitive;
    }
}
