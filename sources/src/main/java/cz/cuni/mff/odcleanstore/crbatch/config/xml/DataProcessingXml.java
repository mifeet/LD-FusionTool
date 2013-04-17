/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import java.util.List;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "DataProcessing")
public class DataProcessingXml {
    @ElementList(name = "Params", required = false, empty = false, inline = true)
    private List<ParamXml> params;
    
    @Element(name = "SeedResourceRestriction", required = false)
    private RestrictionXml seedResourceRestriction;
    
    public List<ParamXml> getParams() {
        return params;
    }

    public RestrictionXml getSeedResourceRestriction() {
        return seedResourceRestriction;
    }

}
