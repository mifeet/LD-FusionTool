/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config.xml;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

import java.util.List;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "DataProcessing")
public class DataProcessingXml {
    @ElementList(name = "Params", required = false, empty = false, inline = true)
    private List<ParamXml> params;
    
    @Element(name = "SeedResourceRestriction", required = false)
    private SeedResourceRestrictionXml seedResourceRestriction;
    
    public List<ParamXml> getParams() {
        return params;
    }

    public SeedResourceRestrictionXml getSeedResourceRestriction() {
        return seedResourceRestriction;
    }

}
