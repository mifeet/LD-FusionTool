/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config.xml;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "DataSource")
public class DataSourceXml extends SourceBaseXml {
    @Element(name = "GraphRestriction", required = false)
    private RestrictionXml graphRestriction;

    public RestrictionXml getGraphRestriction() {
        return graphRestriction;
    }
}
