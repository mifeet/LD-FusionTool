package cz.cuni.mff.odcleanstore.crbatch.config;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;

/**
 * Container of configuration values.
 * @author Jan Michelfeit
 */
public class ConfigImpl implements Config {
    private static final DateFormat CANONICAL_FILE_SUFFIS_FORMAT = new SimpleDateFormat("yyyy.MM.dd-HH.mm.ss");
    
    private String databaseConnectionString;
    private String databaseUsername;
    private String databasePassword;
    private SparqlRestriction namedGraphRestriction;
    private SparqlRestriction ontologyGraphRestriction;
    private SparqlRestriction seedResourceRestriction;
    private String resultDataURIPrefix = ConfigConstants.DEFAULT_RESULT_DATA_URI_PREFIX;
    private List<Output> outputs = new LinkedList<Output>();
    private Map<String, String> prefixes = new HashMap<String, String>();
    private AggregationSpec aggregationSpec;
    private File canonicalURIsOutputFile = 
            new File("canonicalUris-" + CANONICAL_FILE_SUFFIS_FORMAT.format(new Date()) + ".txt");
    private File canonicalURIsInputFile;

    @Override
    public String getDatabaseConnectionString() {
        return databaseConnectionString;
    }

    /**
     * @param databaseConnectionString the databaseConnectionString to set
     */
    public void setDatabaseConnectionString(String databaseConnectionString) {
        this.databaseConnectionString = databaseConnectionString;
    }

    @Override
    public String getDatabaseUsername() {
        return databaseUsername;
    }

    /**
     * @param databaseUsername the databaseUsername to set
     */
    public void setDatabaseUsername(String databaseUsername) {
        this.databaseUsername = databaseUsername;
    }

    @Override
    public String getDatabasePassword() {
        return databasePassword;
    }

    /**
     * @param databasePassword the databasePassword to set
     */
    public void setDatabasePassword(String databasePassword) {
        this.databasePassword = databasePassword;
    }

    @Override
    public SparqlRestriction getNamedGraphRestriction() {
        return namedGraphRestriction;
    }
    
    /**
     * Sets SPARQL group graph pattern limiting source payload named graphs.
     * @param restriction SPARQL group graph pattern
     * @see #getNamedGraphRestrictionPattern()
     */
    public void setNamedGraphRestriction(SparqlRestriction restriction) {
        this.namedGraphRestriction = restriction;
    }
    
    @Override
    public SparqlRestriction getOntologyGraphRestriction() {
        return ontologyGraphRestriction;
    }
    
    /**
     * Sets value for {@link #getOntologyGraphRestrictionPattern()}.
     * @param restriction SPARQL group graph pattern
     */
    public void setOntologyGraphRestriction(SparqlRestriction restriction) {
        this.ontologyGraphRestriction = restriction;
    }

    @Override
    public SparqlRestriction getSeedResourceRestriction() {
        return seedResourceRestriction;
    }
    
    /**
     * Sets value for {@link #getSeedResourceRestrictionPattern()}.
     * @param restriction SPARQL group graph pattern
     */
    public void setSeedResourceRestriction(SparqlRestriction restriction) {
        this.seedResourceRestriction = restriction;
    }

    @Override
    public String getResultDataURIPrefix() {
        return resultDataURIPrefix;
    }
    
    /**
     * Sets prefix of named graphs and URIs where query results and metadata in the output are placed.
     * @param resultDataURIPrefix named graph URI prefix
     */
    public void setResultDataURIPrefix(String resultDataURIPrefix) {
        this.resultDataURIPrefix = resultDataURIPrefix;
    }

    @Override
    public List<Output> getOutputs() {
        return outputs;
    }
    
    /**
     * Sets result outputs.
     * @param outputs list of outputs 
     */
    public void setOutputs(List<Output> outputs) {
        this.outputs = outputs;
    }

    @Override
    public Map<String, String> getPrefixes() {
        return prefixes;
    }
    
    /**
     * Sets map of defined namespace prefixes.
     * @param prefixes map of namespace prefixes
     */
    public void setPrefixes(Map<String, String> prefixes) {
        this.prefixes = prefixes;
    }

    @Override
    public AggregationSpec getAggregationSpec() {
        return aggregationSpec;
    }
    
    /**
     * Sets aggregation settings for conflict resolution.
     * @param aggregationSpec aggregation settings
     */
    public void setAggregationSpec(AggregationSpec aggregationSpec) {
        this.aggregationSpec = aggregationSpec;
    }
    
    @Override
    public File getCanonicalURIsOutputFile() {
        return canonicalURIsOutputFile;
    }
    
    /**
     * Sets file where resolved canonical URIs shall be written. 
     * @param file file to write canonical URIs to
     */
    public void setCanonicalURIsOutputFile(File file) {
        this.canonicalURIsOutputFile = file;
    }

    @Override
    public File getCanonicalURIsInputFile() {
        return canonicalURIsInputFile;
    }
    
    /**
     * Sets file with list of preferred canonical URIs. 
     * @param file file with canonical URIs
     */
    public void setCanonicalURIsInputFile(File file) {
        this.canonicalURIsInputFile = file;
    }

    @Override
    public Integer getQueryTimeout() {
        return ConfigConstants.DEFAULT_QUERY_TIMEOUT;
    }

    @Override
    public Double getAgreeCoeficient() {
        return ConfigConstants.AGREE_COEFFICIENT;
    }

    @Override
    public Double getScoreIfUnknown() {
        return ConfigConstants.SCORE_IF_UNKNOWN;
    }

    @Override
    public Double getNamedGraphScoreWeight() {
        return ConfigConstants.NAMED_GRAPH_SCORE_WEIGHT;
    }

    @Override
    public Double getPublisherScoreWeight() {
        return ConfigConstants.PUBLISHER_SCORE_WEIGHT;
    }

    @Override
    public Long getMaxDateDifference() {
        return ConfigConstants.MAX_DATE_DIFFERENCE;
    }
}
