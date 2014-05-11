package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.Config;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigImpl;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigReader;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConstructSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumOutputType;
import cz.cuni.mff.odcleanstore.fusiontool.config.Output;
import cz.cuni.mff.odcleanstore.fusiontool.config.SourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.InvalidInputException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.simpleframework.xml.core.PersistenceException;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 * The main entry point of the application.
 * @author Jan Michelfeit
 */
public final class ODCSFusionToolApplication {
    /** Parsed command line arguments representation. */
    private static class ApplicationArgs {
        private final boolean isVerbose;
        private final boolean outputConflictsOnly;
        private final boolean outputMappedSubjectsOnly;
        private final boolean isProfilingOn;
        private final String configFilePath;
        
        public ApplicationArgs(boolean isVerbose, boolean isProfilingOn, String configFilePath,
                boolean outputConflictsOnly, boolean outputMappedSubjectsOnly) {
            
            this.isVerbose = isVerbose;
            this.isProfilingOn = isProfilingOn;
            this.configFilePath = configFilePath;
            this.outputConflictsOnly = outputConflictsOnly;
            this.outputMappedSubjectsOnly = outputMappedSubjectsOnly;
        }
        
        public boolean isVerbose() {
            return isVerbose;
        }
        
        public boolean isProfilingOn() {
            return isProfilingOn;
        }
        
        public String getConfigFilePath() {
            return configFilePath;
        }
        
        public boolean getOutputConflictsOnly() {
            return outputConflictsOnly;
        }
        
        public boolean getOutputMappedSubjectsOnly() {
            return outputMappedSubjectsOnly;
        }
    }
    
    private static String getUsage() {
        return "Usage:\n java -jar odcs-fusion-tool-<version>.jar [--verbose] [--profile] [--only-conflicts] [--only-mapped] <xml config file>";
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
            LogManager.getLogger(ODCSFusionToolApplication.class.getPackage().getName()).setLevel(Level.ERROR);
        }
        
        File configFile = new File(parsedArgs.getConfigFilePath());
        if (!configFile.isFile() || !configFile.canRead()) {
            System.err.println("Cannot read the given config file.\n");
            System.err.println(getUsage());
            return;
        }

        Config config;
        try {
            config = ConfigReader.parseConfigXml(configFile);
            ((ConfigImpl) config).setProfilingOn(parsedArgs.isProfilingOn());
            ((ConfigImpl) config).setOutputConflictsOnly(parsedArgs.getOutputConflictsOnly());
            ((ConfigImpl) config).setOutputMappedSubjectsOnly(parsedArgs.getOutputMappedSubjectsOnly());
            checkValidInput(config);
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
        System.out.println("Starting conflict resolution, this may take a while... \n");

        try {
            ODCSFusionToolExecutorRunner odcsFusionToolExecutorRunner = new ODCSFusionToolExecutorRunner(config);
            odcsFusionToolExecutorRunner.runFusionTool();
        } catch (ODCSFusionToolException e) {
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
        System.out.printf("ODCS-FusionTool executed in %s\n", formatRunTime(System.currentTimeMillis() - startTime));
    }
    
    private static ApplicationArgs parseArgs(String[] args) throws InvalidInputException {
        if (args == null) {
            throw new InvalidInputException("Missing command line arguments");
        }
        
        boolean verbose = false;
        boolean outputConflictsOnly = false;
        boolean outputMappedSubjectsOnly = false;
        boolean profile = false;
        String configFilePath = null;
        for (String arg : args) {
            if ("--verbose".equals(arg)) {
                verbose = true;
            } else if ("--profile".equals(arg)) {
                profile = true;
            } else if ("--only-conflicts".equals(arg)) {
                outputConflictsOnly = true;
            } else if ("--only-mapped".equals(arg)) {
                outputMappedSubjectsOnly = true;
            } else {
                configFilePath = arg;
            }
        }
        if (configFilePath == null) {
            throw new InvalidInputException("Missing config file argument");
        }
        return new ApplicationArgs(verbose, profile, configFilePath, outputConflictsOnly, outputMappedSubjectsOnly);
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
        if (config.getDataSources() == null || config.getDataSources().isEmpty()) {
            throw new InvalidInputException("There must be at least one DataSource specified");
        }
        for (DataSourceConfig dataSourceConfig : config.getDataSources()) {
            checkDataSourceValidInput(dataSourceConfig, config);
        }
        
        for (ConstructSourceConfig sourceConfig : config.getSameAsSources()) {
            checkConstructSourceValidInput(sourceConfig, config);
        }
        for (ConstructSourceConfig sourceConfig : config.getMetadataSources()) {
            checkConstructSourceValidInput(sourceConfig, config);
        }
        
        // Check output settings
        for (Output output : config.getOutputs()) {
            if (output.getType() == EnumOutputType.FILE && output.getParams().get(ConfigParameters.OUTPUT_PATH) != null) {
                File fileLocation = new File(output.getParams().get(ConfigParameters.OUTPUT_PATH));
                if (fileLocation.exists() && !fileLocation.canWrite()) {
                    throw new InvalidInputException("Cannot write to output file " + fileLocation.getPath());
                }
            }
        }

        // intentionally do not check canonical URI files
    }

    private static void checkConstructSourceValidInput(ConstructSourceConfig sourceConfig, Config config) throws InvalidInputException {
        checkSourceValidInput(sourceConfig, config);
    }

    private static void checkDataSourceValidInput(DataSourceConfig dataSourceConfig, Config config) throws InvalidInputException {
        checkSourceValidInput(dataSourceConfig, config);
        
        if (!ODCSUtils.isValidSparqlVar(dataSourceConfig.getNamedGraphRestriction().getVar())) {
            throw new InvalidInputException(
                    "Variable name specified in source graphs restriction must be a valid SPARQL identifier, '"
                            + dataSourceConfig.getNamedGraphRestriction().getVar() 
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
    }

    private static void checkSourceValidInput(SourceConfig sourceConfig, Config config) throws InvalidInputException {
        switch (sourceConfig.getType()) {
        case VIRTUOSO:
            checkRequiredDataSourceParam(sourceConfig,
                    ConfigParameters.DATA_SOURCE_VIRTUOSO_HOST,
                    ConfigParameters.DATA_SOURCE_VIRTUOSO_PORT,
                    ConfigParameters.DATA_SOURCE_VIRTUOSO_USERNAME,
                    ConfigParameters.DATA_SOURCE_VIRTUOSO_PASSWORD);
            break;
        case SPARQL:
            checkRequiredDataSourceParam(sourceConfig, ConfigParameters.DATA_SOURCE_SPARQL_ENDPOINT);
            break;
        case FILE:
            checkRequiredDataSourceParam(sourceConfig, ConfigParameters.DATA_SOURCE_FILE_PATH);
            File file = new File(sourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_FILE_PATH));
            if (!file.isFile() || !file.canRead()) {
                throw new InvalidInputException("Cannot read input file " + sourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_FILE_PATH));
            }
            break;
        default:
            throw new InvalidInputException("Unsupported type of data source: " + sourceConfig.getType());
        }
    }
    
    private static void checkRequiredDataSourceParam(SourceConfig dataSourceConfig, String... requiredParams)
            throws InvalidInputException {
        
        for (String requiredParam : requiredParams) {
            if (dataSourceConfig.getParams().get(requiredParam) == null) {
                throw new InvalidInputException("Missing required parameter '" + requiredParam
                        + "' for data source " + dataSourceConfig);
            }
        }
    }
    
    private static String formatRunTime(long runTime) {
        final long hourMs = ODCSUtils.MILLISECONDS * ODCSUtils.TIME_UNIT_60 * ODCSUtils.TIME_UNIT_60;
        DateFormat timeFormat = new SimpleDateFormat("mm:ss.SSS");
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return String.format("%d:%s",
                runTime / hourMs,
                timeFormat.format(new Date(runTime)));
    }

    /** Disable constructor. */
    private ODCSFusionToolApplication() {
    }
}
