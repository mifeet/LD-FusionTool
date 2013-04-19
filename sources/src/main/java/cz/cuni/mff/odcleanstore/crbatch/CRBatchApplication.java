package cz.cuni.mff.odcleanstore.crbatch;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.simpleframework.xml.core.PersistenceException;

import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.crbatch.config.Config;
import cz.cuni.mff.odcleanstore.crbatch.config.ConfigReader;
import cz.cuni.mff.odcleanstore.crbatch.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.crbatch.config.DataSourceConfigImpl;
import cz.cuni.mff.odcleanstore.crbatch.config.Output;
import cz.cuni.mff.odcleanstore.crbatch.config.SparqlRestrictionImpl;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.InvalidInputException;
import cz.cuni.mff.odcleanstore.crbatch.util.CRBatchUtils;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;

/**
 * The main entry point of the application.
 * @author Jan Michelfeit
 */
public final class CRBatchApplication {
    /** Parsed command line arguments representation. */
    private static class ApplicationArgs {
        private final boolean isVerbose;
        private final String configFilePath;
        
        public ApplicationArgs(boolean isVerbose, String configFilePath) {
            this.isVerbose = isVerbose;
            this.configFilePath = configFilePath;
        }
        
        public boolean isVerbose() {
            return isVerbose;
        }
        
        public String getConfigFilePath() {
            return configFilePath;
        }
    }
    
    private static String getUsage() {
        return "Usage:\n java -jar odcs-cr-batch-<version>.jar [--verbose] <config file>.xml";
    }

    /**
     * Main application entry point.
     * @param args command line arguments
     */
    public static void main(String[] args) {
        ApplicationArgs parsedArgs;
        try {
            parsedArgs = parseArgs(args);
        } catch (InvalidInputException e) {
            System.err.println(getUsage());
            return;
        }
        
        if (!parsedArgs.isVerbose()) {
            LogManager.getLogger(CRBatchApplication.class.getPackage().getName()).setLevel(Level.ERROR);
        }
        
        File configFile = new File(parsedArgs.getConfigFilePath());
        if (!configFile.isFile() || !configFile.canRead()) {
            System.err.println("Cannot read the given config file.\n");
            System.err.println(getUsage());
            return;
        }

        Config config = null;
        try {
            config = ConfigReader.parseConfigXml(configFile);
            checkValidInput(config);
            setupDefaultRestrictions(config);
        } catch (InvalidInputException e) {
            System.err.println("Error in config file:");
            System.err.println("  " + e.getMessage());
            if (e.getCause() instanceof PersistenceException) {
                System.err.println("  " + e.getCause().getMessage());
            }
            e.printStackTrace();
            return;
        }

        long startTime = System.currentTimeMillis();
        System.out.println("Starting conflict resolution batch, this may take a while... \n");

        try {
            CRBatchExecutor crBatchExecutor = new CRBatchExecutor();
            crBatchExecutor.runCRBatch(config);
        } catch (CRBatchException e) {
            System.err.println("Error:");
            System.err.println("  " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("  " + e.getCause().getMessage());
            }
            return;
        } catch (ConflictResolutionException e) {
            System.err.println("Conflict resolution error:");
            System.err.println("  " + e.getMessage());
            return;
        } catch (IOException e) {
            System.err.println("Error when writing results:");
            System.err.println("  " + e.getMessage());
            return;
        }

        System.out.println("----------------------------");
        
        long runTime = System.currentTimeMillis() - startTime;
        final long hourMs = ODCSUtils.MILLISECONDS * ODCSUtils.TIME_UNIT_60 * ODCSUtils.TIME_UNIT_60;
        DateFormat timeFormat = new SimpleDateFormat("mm:ss.SSS");
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        System.out.printf("CR-batch executed in %d:%s \n",
                runTime / hourMs,
                timeFormat.format(new Date(runTime)));
    }
    
    private static ApplicationArgs parseArgs(String[] args) throws InvalidInputException {
        if (args == null) {
            throw new InvalidInputException("Missing command line arguments");
        }
        
        boolean verbose = false;
        int configFilePathPosition = 0;
        if (configFilePathPosition < args.length && "--verbose".equals(args[configFilePathPosition])) {
            verbose = true;
            configFilePathPosition++;
        }
        if (configFilePathPosition >= args.length) {
            throw new InvalidInputException("Missing config file argument");
        }
        return new ApplicationArgs(verbose, args[configFilePathPosition]);
    }

    private static void checkValidInput(Config config) throws InvalidInputException {
        if (!ODCSUtils.isValidIRI(config.getResultDataURIPrefix())) {
            throw new InvalidInputException("Result data URI prefix must be a valid URI, '" + config.getResultDataURIPrefix()
                    + "' given");
        }
        for (Map.Entry<String, String> prefixEntry : config.getPrefixes().entrySet()) {
            if (!prefixEntry.getKey().isEmpty() && !ODCSUtils.isValidNamespacePrefix(prefixEntry.getKey())) {
                throw new InvalidInputException("Invalid namespace prefix '" + prefixEntry.getKey() + "'");
            }
            if (!prefixEntry.getValue().isEmpty() && !ODCSUtils.isValidIRI(prefixEntry.getValue())) {
                throw new InvalidInputException("Invalid namespace prefix definition for URI '" + prefixEntry.getValue() + "'");
            }
        }
        if (config.getSeedResourceRestriction() != null
                && !ODCSUtils.isValidSparqlVar(config.getSeedResourceRestriction().getVar())) {
            throw new InvalidInputException(
                    "Variable name specified in seed resources restriction must be a valid SPARQL identifier, '"
                            + config.getSeedResourceRestriction().getVar() + "' given");
        }   
       
        // Check data Source settings
        for (DataSourceConfig dataSourceConfig : config.getDataSources()) {
            checkDataSourceValidInput(dataSourceConfig, config);
        }
        
        // Check output settings
        for (Output output : config.getOutputs()) {
            if (output.getFileLocation().exists() && !output.getFileLocation().canWrite()) {
                System.out.println(output.getFileLocation().getAbsolutePath());
                throw new InvalidInputException("Cannot write to output file " + output.getFileLocation().getPath());
            }
        }

        // intentionally do not check canonical URI files
    }

    private static void checkDataSourceValidInput(DataSourceConfig dataSourceConfig, Config config) throws InvalidInputException {
        if (!ODCSUtils.isValidSparqlVar(dataSourceConfig.getNamedGraphRestriction().getVar())) {
            throw new InvalidInputException(
                    "Variable name specified in source graphs restriction must be a valid SPARQL identifier, '"
                            + dataSourceConfig.getNamedGraphRestriction().getVar() 
                            + "' given for data source " + dataSourceConfig);
        }
        if (dataSourceConfig.getMetadataGraphRestriction() != null
                && !ODCSUtils.isValidSparqlVar(dataSourceConfig.getMetadataGraphRestriction().getVar())) {
            throw new InvalidInputException(
                    "Variable name specified in ontology graphs restriction must be a valid SPARQL identifier, '"
                            + dataSourceConfig.getMetadataGraphRestriction().getVar() 
                            + "' given for data source " + dataSourceConfig);
        }
        if (config.getSeedResourceRestriction() != null
                && dataSourceConfig.getNamedGraphRestriction().getVar().equals(
                        config.getSeedResourceRestriction().getVar())) {
            String message = "SPARQL variable used in source named graph restriction (<GraphRestriction var=\"...\" />) "
                    + "and variable used in seed resource restriction (<SeedResourceRestriction var=\"...\" />)"
                    + " must be different, but both are set to ?" + config.getSeedResourceRestriction().getVar() + "."
                    + "\nNote that any other variables used in the two restriction patterns should be also different.";
            throw new InvalidInputException(message);
        }
        
        switch (dataSourceConfig.getType()) {
        case VIRTUOSO:
            checkRequiredDataSourceParam(dataSourceConfig, "host", "port", "username", "password");
            break;
        case SPARQL:
            checkRequiredDataSourceParam(dataSourceConfig, "endpointurl");
            break;
        case FILE:
            checkRequiredDataSourceParam(dataSourceConfig, "path");
            File file = new File(dataSourceConfig.getParams().get("path"));
            if (!file.isFile() || !file.canRead()) {
                throw new InvalidInputException("Cannot read input file " + dataSourceConfig.getParams().get("path"));
            }
            break;
        default:
            throw new InvalidInputException("Unsupported type of data source: " + dataSourceConfig.getType());
        }
    }

    private static void checkRequiredDataSourceParam(DataSourceConfig dataSourceConfig, String... requiredParams)
            throws InvalidInputException {
        
        for (String requiredParam : requiredParams) {
            if (dataSourceConfig.getParams().get(requiredParam) == null) {
                throw new InvalidInputException("Missing required parameter '" + requiredParam
                        + "' for data source " + dataSourceConfig);
            }
        }
    }
    
    /**
     * Sets up SPARQL restrictions in config if none are given.
     * This step is mostly for performance reasons, e.g. loading metadata from a large
     * RDF data source without a restriction on metadata graphs is to demanding.
     * @param config
     */
    private static void setupDefaultRestrictions(Config config) {
        final String varPrefix = "c6fa378ae1_"; // some random string to avoid collisions
        for (DataSourceConfig dsConfigImmutable : config.getDataSources()) {
            DataSourceConfigImpl dsConfig = (DataSourceConfigImpl) dsConfigImmutable;
            switch (dsConfig.getType()) {
            case SPARQL:
            case VIRTUOSO:
                // Use ODCS default if appropriate
                if (CRBatchUtils.isRestrictionEmpty(dsConfig.getMetadataGraphRestriction())) {
                    // { SELECT ?m WHERE {?g odcs:metadataGraph ?m} }
                    // UNION { SELECT ?m WHERE {?m odcs:generatedGraph 1} }
                    // UNION { SELECT ?m WHERE { GRAPH ?m {?p odcs:publisherScore ?s}} }
                    String pattern = " { SELECT ?" + varPrefix + "m"
                            + "\n        WHERE {?" + varPrefix + "g <" + ODCS.metadataGraph + "> ?" + varPrefix + "m} }"
                            + "\n      UNION " + "{ SELECT ?" + varPrefix + "m"
                            + "\n        WHERE {?" + varPrefix + "m <" + ODCS.generatedGraph + "> 1} }"
                            + "\n      UNION { SELECT ?" + varPrefix + "m"
                            + "\n        WHERE { GRAPH ?" + varPrefix + "m {"
                            + "\n                ?" + varPrefix + "p <" + ODCS.publisherScore + "> ?" + varPrefix + "s} } }";
                    dsConfig.setMetadataGraphRestriction(new SparqlRestrictionImpl(pattern, varPrefix + "m"));
                }
                break;
            default:
                // do nothing
                break;
            }
        }
    }

    /** Disable constructor. */
    private CRBatchApplication() {
    }
}
