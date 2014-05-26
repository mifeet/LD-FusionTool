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
@Root(name = "Config")
public class ConfigXml {

    @ElementList(name = "Prefixes", required = false)
    private List<PrefixXml> prefixes;

    @Element(name = "Sources", required = true)
    private SourcesXml sources;
    
    @Element(name="DataProcessing", required = false)
    private DataProcessingXml dataProcessing;

    @Element(name = "ConflictResolution", required = false)
    private ConflictResolutionXml conflictResolution;

    @ElementList(name = "Outputs")
    private List<OutputXml> outputs;

    public List<PrefixXml> getPrefixes() {
        return prefixes;
    }

    public SourcesXml getSources() {
        return sources;
    }
    
    public DataProcessingXml getDataProcessing() {
        return dataProcessing;
    }

    public ConflictResolutionXml getConflictResolution() {
        return conflictResolution;
    }

    public List<OutputXml> getOutputs() {
        return outputs;
    }
}
