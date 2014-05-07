package cz.cuni.mff.odcleanstore.fusiontool.config;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolutionStrategyImpl;
import org.openrdf.model.URI;

import java.io.File;
import java.util.*;

/**
 * Container of configuration values.
 *
 * @author Jan Michelfeit
 */
public class ConfigImpl implements Config {
    private List<DataSourceConfig> dataSources = Collections.emptyList();
    private List<ConstructSourceConfig> sameAsSources = Collections.emptyList();
    private List<ConstructSourceConfig> metadataSources = Collections.emptyList();
    private SparqlRestriction seedResourceRestriction;
    private String resultDataURIPrefix = ConfigConstants.DEFAULT_RESULT_DATA_URI_PREFIX;
    private List<Output> outputs = new LinkedList<Output>();
    private Map<String, String> prefixes = new HashMap<String, String>();
    private ResolutionStrategy defaultResolutionStrategy = new ResolutionStrategyImpl();
    private Map<URI, ResolutionStrategy> propertyResolutionStrategies = new HashMap<URI, ResolutionStrategy>();

    private File canonicalURIsOutputFile = null;
    private File canonicalURIsInputFile;
    private boolean enableFileCache = false;
    private Long maxOutputTriples = null;
    private boolean isProfilingOn = false;
    private boolean outputConflictsOnly = false;
    private boolean outputMappedSubjectsOnly = false;
    private boolean isProcessingTransitive = true;

    @Override
    public List<DataSourceConfig> getDataSources() {
        return dataSources;
    }

    /**
     * Sets data source settings.
     *
     * @param dataSources settings for data sources
     */
    public void setDataSources(List<DataSourceConfig> dataSources) {
        this.dataSources = dataSources;
    }

    @Override
    public List<ConstructSourceConfig> getSameAsSources() {
        return sameAsSources;
    }

    /**
     * Sets value for {@link #getSameAsSources()}.
     *
     * @param sameAsSources settings for owl:sameAs sources
     */
    public void setSameAsSources(List<ConstructSourceConfig> sameAsSources) {
        this.sameAsSources = sameAsSources;
    }

    @Override
    public List<ConstructSourceConfig> getMetadataSources() {
        return metadataSources;
    }

    /**
     * Sets value for {@link #getMetadataSources()}.
     *
     * @param metadataSources settings metadata sources
     */
    public void setMetadataSources(List<ConstructSourceConfig> metadataSources) {
        this.metadataSources = metadataSources;
    }

    @Override
    public SparqlRestriction getSeedResourceRestriction() {
        return seedResourceRestriction;
    }

    /**
     * Sets value for {@link #getSeedResourceRestriction()}.
     *
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
     *
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
     *
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
     *
     * @param prefixes map of namespace prefixes
     */
    public void setPrefixes(Map<String, String> prefixes) {
        this.prefixes = prefixes;
    }

    @Override
    public ResolutionStrategy getDefaultResolutionStrategy() {
        return defaultResolutionStrategy;
    }

    /**
     * Setter for value of {@link #getDefaultResolutionStrategy()}.
     *
     * @param strategy conflict resolution strategy
     */
    public void setDefaultResolutionStrategy(ResolutionStrategy strategy) {
        this.defaultResolutionStrategy = strategy;
    }

    @Override
    public Map<URI, ResolutionStrategy> getPropertyResolutionStrategies() {
        return propertyResolutionStrategies;
    }

    /**
     * Setter for value of {@link #getDefaultResolutionStrategy()}.
     *
     * @param strategies per-property conflict resolution strategies
     */
    public void setPropertyResolutionStrategies(Map<URI, ResolutionStrategy> strategies) {
        this.propertyResolutionStrategies = strategies;
    }

    @Override
    public File getCanonicalURIsOutputFile() {
        return canonicalURIsOutputFile;
    }

    /**
     * Sets file where resolved canonical URIs shall be written.
     *
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
     *
     * @param file file with canonical URIs
     */
    public void setCanonicalURIsInputFile(File file) {
        this.canonicalURIsInputFile = file;
    }

    @Override
    public boolean getEnableFileCache() {
        return enableFileCache;
    }

    /**
     * Sets value for {@link #getEnableFileCache()}.
     *
     * @param enableFileCache see {@link #getEnableFileCache()}
     */
    public void setEnableFileCache(boolean enableFileCache) {
        this.enableFileCache = enableFileCache;
    }

    @Override
    public Long getMaxOutputTriples() {
        return maxOutputTriples;
    }

    /**
     * Sets value for {@link #getMaxOutputTriples()}.
     *
     * @param maxOutputTriples see {@link #getMaxOutputTriples()}
     */
    public void setMaxOutputTriples(Long maxOutputTriples) {
        this.maxOutputTriples = maxOutputTriples;
    }

    @Override
    public boolean isProfilingOn() {
        return isProfilingOn;
    }

    /**
     * Sets value for {@link #isProfilingOn()}.
     *
     * @param isProfilingOn see {@link #isProfilingOn()}
     */
    public void setProfilingOn(boolean isProfilingOn) {
        this.isProfilingOn = isProfilingOn;
    }

    @Override
    public boolean getOutputConflictsOnly() {
        return outputConflictsOnly;
    }

    /**
     * Sets value for {@link #getOutputConflictsOnly()}.
     *
     * @param outputConflictsOnly see {@link #getOutputConflictsOnly()}
     */
    public void setOutputConflictsOnly(boolean outputConflictsOnly) {
        this.outputConflictsOnly = outputConflictsOnly;
    }

    @Override
    public boolean getOutputMappedSubjectsOnly() {
        return outputMappedSubjectsOnly;
    }

    /**
     * Sets value for {@link #getOutputMappedSubjectsOnly()}.
     *
     * @param outputMappedSubjectsOnly see {@link #getOutputMappedSubjectsOnly()}
     */
    public void setOutputMappedSubjectsOnly(boolean outputMappedSubjectsOnly) {
        this.outputMappedSubjectsOnly = outputMappedSubjectsOnly;
    }

    @Override
    public boolean isProcessingTransitive() {
        return isProcessingTransitive;
    }

    /**
     * Sets value for {@link #isProcessingTransitive()}.
     *
     * @param isProcessingTransitive see {@link #isProcessingTransitive()}
     */
    public void setIsProcessingTransitive(boolean isProcessingTransitive) {
        this.isProcessingTransitive = isProcessingTransitive;
    }

    @Override
    public Integer getQueryTimeout() {
        return ConfigConstants.DEFAULT_QUERY_TIMEOUT;
    }

    @Override
    public Double getAgreeCoefficient() {
        return ConfigConstants.AGREE_COEFFICIENT;
    }

    @Override
    public Double getScoreIfUnknown() {
        return ConfigConstants.SCORE_IF_UNKNOWN;
    }

    @Override
    public Double getPublisherScoreWeight() {
        return ConfigConstants.PUBLISHER_SCORE_WEIGHT;
    }

    @Override
    public Long getMaxDateDifference() {
        return ConfigConstants.MAX_DATE_DIFFERENCE;
    }

    @Override
    public Collection<String> getPreferredCanonicalURIs() {
        return ConfigConstants.DEFAULT_PREFERRED_CANONICAL_URIS;
    }

    @Override
    public File getWorkingDirectory() {
        return ConfigConstants.WORKING_DIRECTORY;
    }

    @Override
    public int getSparqlResultMaxRows() {
        return ConfigConstants.SPARQL_RESULT_MAX_ROWS;
    }
}


