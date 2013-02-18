/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import java.util.List;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

/**
 * @author Jan Michelfeit
 */
@Root(name = "Config")
public class ConfigXml {

    @ElementList(name = "Prefixes", required = false)
    private List<PrefixXml> prefixes;

    @ElementList(name = "DataSource")
    private List<ParamXml> dataSource;

    @Element(name = "SourceGraphsRestriction", required = false)
    private SourceGraphsRestrictionXml sourceGraphsRestriction;

    @Element(name = "ConflictResolution", required = false)
    private ConflictResolutionXml conflictResolution;

    @ElementList(name = "Outputs")
    private List<OutputXml> outputs;

    public List<PrefixXml> getPrefixes() {
        return prefixes;
    }

    public List<ParamXml> getDataSource() {
        return dataSource;
    }

    public SourceGraphsRestrictionXml getSourceGraphsRestriction() {
        return sourceGraphsRestriction;
    }

    public ConflictResolutionXml getConflictResolution() {
        return conflictResolution;
    }

    public List<OutputXml> getOutputs() {
        return outputs;
    }
}
