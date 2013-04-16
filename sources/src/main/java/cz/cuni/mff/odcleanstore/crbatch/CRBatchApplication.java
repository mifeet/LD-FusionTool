package cz.cuni.mff.odcleanstore.crbatch;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.simpleframework.xml.core.PersistenceException;

import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.crbatch.config.Config;
import cz.cuni.mff.odcleanstore.crbatch.config.ConfigReader;
import cz.cuni.mff.odcleanstore.crbatch.config.Output;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.InvalidInputException;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;

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
            //Repository myRepository = new SailRepository(new MemoryStore());
            //Repository myRepository = new SPARQLRepository("http://localhost:8890/sparql");
            //con.add(file, baseURI, RDFFormat.RDFXML);
        
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
        System.out.printf("CR-batch executed in %.3f s\n",
                (System.currentTimeMillis() - startTime) / (double) ODCSUtils.MILLISECONDS);
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
        for (Output output : config.getOutputs()) {
            if (output.getFileLocation().exists() && !output.getFileLocation().canWrite()) {
                System.out.println(output.getFileLocation().getAbsolutePath());
                throw new InvalidInputException("Cannot write to output file " + output.getFileLocation().getPath());
            }
        }
        for (Map.Entry<String, String> prefixEntry : config.getPrefixes().entrySet()) {
            if (!prefixEntry.getKey().isEmpty() && !ODCSUtils.isValidNamespacePrefix(prefixEntry.getKey())) {
                throw new InvalidInputException("Invalid namespace prefix '" + prefixEntry.getKey() + "'");
            }
            if (!prefixEntry.getValue().isEmpty() && !ODCSUtils.isValidIRI(prefixEntry.getValue())) {
                throw new InvalidInputException("Invalid namespace prefix definition for URI '" + prefixEntry.getValue() + "'");
            }
        }
        if (!ODCSUtils.isValidSparqlVar(config.getNamedGraphRestriction().getVar())) {
            throw new InvalidInputException(
                    "Variable name specified in source graphs restriction must be a valid SPARQL identifier, '"
                            + config.getNamedGraphRestriction().getVar() + "' given");
        }
        if (config.getOntologyGraphRestriction() != null
                && !ODCSUtils.isValidSparqlVar(config.getOntologyGraphRestriction().getVar())) {
            throw new InvalidInputException(
                    "Variable name specified in ontology graphs restriction must be a valid SPARQL identifier, '"
                            + config.getOntologyGraphRestriction().getVar() + "' given");
        }
        if (config.getSeedResourceRestriction() != null
                && !ODCSUtils.isValidSparqlVar(config.getSeedResourceRestriction().getVar())) {
            throw new InvalidInputException(
                    "Variable name specified in seed resources restriction must be a valid SPARQL identifier, '"
                            + config.getSeedResourceRestriction().getVar() + "' given");
        }   
        if (config.getSeedResourceRestriction() != null
                && config.getNamedGraphRestriction().getVar().equals(config.getSeedResourceRestriction().getVar())) {
            String message = "SPARQL variable used in source named graph restriction (<GraphsRestriction var=\"...\" />) "
                    + "and variable used in seed resource restriction (<SeedResourceRestriction var=\"...\" />)"
                    + " must be different, but both are set to ?" + config.getNamedGraphRestriction().getVar() + "."
                    + "\nNote that any other variables used in the two restriction patterns should be also different.";
            throw new InvalidInputException(message);
        }
        // intentionally do not check canonical URI files
    }

    /** Disable constructor. */
    private CRBatchApplication() {
    }
}
