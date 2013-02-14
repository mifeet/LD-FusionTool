package cz.cuni.mff.odcleanstore.crbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
import cz.cuni.mff.odcleanstore.crbatch.loaders.NamedGraphLoader;
import cz.cuni.mff.odcleanstore.crbatch.loaders.QueryUtils;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;

/**
 * The main entry point of the application.
 * @author Jan Michelfeit
 */
public final class Application {
    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    /**
     * @param args
     * 
     */
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        Config config = new Config();
        config.setDatabaseConnectionString("jdbc:virtuoso://localhost:1111/CHARSET=UTF-8");
        config.setDatabasePassword("dba");
        config.setDatabaseUsername("dba");
        config.setNamedGraphConstraintPattern(QueryUtils.preprocessGroupGraphPattern(
                        ConfigConstants.NG_CONSTRAINT_PATTERN_VARIABLE + " <" + ODCS.isLatestUpdate + "> 1"));
//        config.setNamedGraphConstraintPattern(QueryUtils.preprocessGroupGraphPattern(
//                        ConfigConstants.NG_CONSTRAINT_PATTERN_VARIABLE + " <" + ODCS.metadataGraph + "> ?x"));
//        
        
        ConnectionFactory connectionFactory = new ConnectionFactory(config);
        NamedGraphLoader graphLoader = new NamedGraphLoader(connectionFactory, config.getNamedGraphConstraintPattern());
        try {
            NamedGraphMetadataMap ngs = graphLoader.getNamedGraphs();
            System.out.println(ngs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        LOG.debug("----------------------------");
        LOG.debug("CR-batch executed in {} ms", System.currentTimeMillis() - startTime);
    }
    
    // TODO
//  private static String buildPublisherList(Collection<String> publishers) throws InvalidInputException {
//      if (publishers.isEmpty()) {
//          return null;
//      }
//
//      StringBuilder listBuilder = new StringBuilder();
//      for (String publisher : publishers) {
//          if (!ODCSUtils.isValidIRI(publisher)) {
//              throw new InvalidInputException("Publisher '" + publisher + "' is not a valid URI");
//          }
//          if (listBuilder.length() > 0) {
//              listBuilder.append(", ");
//          }
//          listBuilder.append('<').append(publisher).append('>');
//      }
//      return listBuilder.toString();
//  }

    
    /** Disable constructor. */
    private Application() {
    }
}
