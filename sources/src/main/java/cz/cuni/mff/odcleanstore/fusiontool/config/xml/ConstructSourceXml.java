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
@Root
public class ConstructSourceXml extends SourceBaseXml {
    @Element(name = "ConstructQuery", required = true)
    private String constructQuery;

    public String getConstructQuery() {
        return constructQuery;
    }
}
