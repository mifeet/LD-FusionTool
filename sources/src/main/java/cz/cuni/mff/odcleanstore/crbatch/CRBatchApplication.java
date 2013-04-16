package cz.cuni.mff.odcleanstore.crbatch;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.openrdf.model.util.URIUtil;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.simpleframework.xml.core.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import virtuoso.sesame2.driver.VirtuosoRepository;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadata;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.crbatch.config.Config;
import cz.cuni.mff.odcleanstore.crbatch.config.ConfigImpl;
import cz.cuni.mff.odcleanstore.crbatch.config.ConfigReader;
import cz.cuni.mff.odcleanstore.crbatch.config.Output;
import cz.cuni.mff.odcleanstore.crbatch.config.OutputImpl;
import cz.cuni.mff.odcleanstore.crbatch.config.QueryConfig;
import cz.cuni.mff.odcleanstore.crbatch.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.crbatch.config.SparqlRestrictionImpl;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.InvalidInputException;
import cz.cuni.mff.odcleanstore.crbatch.io.SplitFileNameGenerator;
import cz.cuni.mff.odcleanstore.crbatch.loaders.NamedGraphLoader;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;

/**
 * The main entry point of the application.
 * @author Jan Michelfeit
 */
public final class CRBatchApplication {
    private static final Logger LOG = LoggerFactory.getLogger(CRBatchApplication.class);
    
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
            if (config.getProcessByPublishers()) {
                Set<String> publishers = listPublishers(config);
                LOG.info("Identified {} publishers", publishers.size());
                int i = 0;
                for (String publisher : publishers) {
                    System.out.println("Executing for publisher <" + publisher + ">");
                    Config publisherConfig = getConfigForPublisher(publisher, ++i, config);
                    execute(publisherConfig);
                }
            } else {
                execute(config);
            }
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

    private static void execute(Config config) throws CRBatchException, ConflictResolutionException, IOException {
        CRBatchExecutor crBatchExecutor = new CRBatchExecutor();
        crBatchExecutor.runCRBatch(config);
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

    // This is a little hacked together; probably to be removed in future versions
    private static Config getConfigForPublisher(String publisher, int publisherIndex, Config config) {
        ConfigImpl publisherConfig = (ConfigImpl) config.shallowClone();
        
        // Limit named graph restriction pattern to the given publisher
        SparqlRestriction oldRestriction = config.getNamedGraphRestriction();
        String publisherTriplePattern = "?" + oldRestriction.getVar() + " <" + ODCS.publishedBy + "> <" + publisher + ">.";
        SparqlRestriction newRestriction = new SparqlRestrictionImpl(
                publisherTriplePattern + "\n" + oldRestriction.getPattern(),
                oldRestriction.getVar());
        publisherConfig.setNamedGraphRestriction(newRestriction);
        
        // Adjust outputs for the given publisher
        String outputSuffix = Integer.toString(publisherIndex);
        outputSuffix += getPublisherSuffix(publisher);
        List<Output> oldOutputs = config.getOutputs();
        List<Output> newOutputs = new LinkedList<Output>();
        for (Output oldOutput : oldOutputs) {
            switch (oldOutput.getFormat()) {
            case N3:
            case RDF_XML:
                SplitFileNameGenerator outputFileNameGenerator = new SplitFileNameGenerator(oldOutput.getFileLocation());
                OutputImpl newOutput = new OutputImpl(oldOutput.getFormat(), outputFileNameGenerator.nextFile(outputSuffix));
                newOutput.setSplitByBytes(oldOutput.getSplitByBytes());
                if (oldOutput.getSameAsFileLocation() != null) {
                    SplitFileNameGenerator sameAsFileNameGenerator =
                            new SplitFileNameGenerator(oldOutput.getSameAsFileLocation());
                    newOutput.setSameAsFileLocation(sameAsFileNameGenerator.nextFile(outputSuffix));
                }
                newOutputs.add(newOutput);
                break;
            default:
                newOutputs.add(oldOutput);
                break;
            }
        }
        publisherConfig.setOutputs(newOutputs);
        
        return publisherConfig;
    }

    private static String getPublisherSuffix(String publisher) {
        try {
            int namespaceIndex = URIUtil.getLocalNameIndex(publisher);
            if (namespaceIndex < publisher.length()) {
                return "-" + publisher.substring(namespaceIndex).replace('.', '_');
            }
        } catch (IllegalArgumentException e) {
            // ignore
        }
        return "";
    }

    private static Set<String> listPublishers(Config config) throws CRBatchException {
        Repository repository = new VirtuosoRepository(
                config.getDatabaseConnectionString(),
                config.getDatabaseUsername(),
                config.getDatabasePassword());
        try {
            repository.initialize();
            // Load source named graphs metadata
            NamedGraphLoader graphLoader = new NamedGraphLoader(repository, (QueryConfig) config);
            NamedGraphMetadataMap namedGraphsMetadata = graphLoader.getNamedGraphs();

            Set<String> publishers = new HashSet<String>();
            for (NamedGraphMetadata metadata : namedGraphsMetadata.listMetadata()) {
                for (String publisher : metadata.getPublishers()) {
                    publishers.add(publisher);
                }
            }
            return publishers;
        } catch (RepositoryException e) {
            throw new CRBatchException(CRBatchErrorCodes.REPOSITORY_INIT, "Error when initializing repository", e);
        } finally {
            try {
                repository.shutDown();
            } catch (RepositoryException e) {
                // ignore
            }
        }
    }

    /** Disable constructor. */
    private CRBatchApplication() {
    }
}
