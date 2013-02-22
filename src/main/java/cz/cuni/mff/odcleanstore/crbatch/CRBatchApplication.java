package cz.cuni.mff.odcleanstore.crbatch;

import java.io.File;
import java.io.IOException;

import org.simpleframework.xml.core.PersistenceException;

import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.crbatch.config.Config;
import cz.cuni.mff.odcleanstore.crbatch.config.ConfigReader;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.InvalidInputException;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;

/**
 * The main entry point of the application.
 * @author Jan Michelfeit
 */
public final class CRBatchApplication {
    private static String getUsage() {
        return "Usage:\n java -jar odcs-cr-batch-<version>.jar <config file>.xml";
    }

    /**
     * Main application entry point.
     * @param args command line arguments
     */
    public static void main(String[] args) {
        if (args == null || args.length < 1) {
            System.err.println(getUsage());
            return;
        }
        File configFile = new File(args[0]);
        if (!configFile.isFile() || !configFile.canRead()) {
            System.err.println("Cannot read the given config file.\n");
            System.err.println(getUsage());
            return;
        }

        Config config = null;
        try {
            config = ConfigReader.parseConfigXml(configFile);
        } catch (InvalidInputException e) {
            System.err.println("Error in config file:");
            System.err.println("  " + e.getMessage());
            if (e.getCause() instanceof PersistenceException) {
                System.err.println("  " + e.getCause().getMessage());
            }
            e.printStackTrace();
            return;
        }

        // TODO: check valid input (incl. validity of prefixes)
        
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

    /** Disable constructor. */
    private CRBatchApplication() {
    }
}
