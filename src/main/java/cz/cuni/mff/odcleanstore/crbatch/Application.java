package cz.cuni.mff.odcleanstore.crbatch;

import java.sql.SQLException;

import cz.cuni.mff.odcleanstore.connection.VirtuosoConnectionWrapper;
import cz.cuni.mff.odcleanstore.connection.WrappedResultSet;
import cz.cuni.mff.odcleanstore.connection.exceptions.DatabaseException;

/**
 * The main entry point of the application.
 * @author Jan Michelfeit
 */
public final class Application {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        System.out.println("It works!");

        Config config = new Config();
        config.setDatabaseConnectionString("jdbc:virtuoso://localhost:1111/CHARSET=UTF-8");
        config.setDatabasePassword("dba");
        config.setDatabaseUsername("dba");

        
        ConnectionFactory connectionFactory = new ConnectionFactory(config);
        VirtuosoConnectionWrapper conn;
        try {
            conn = connectionFactory.createConnection();
            WrappedResultSet result = conn.executeSelect("SPARQL SELECT ?p WHERE { <a> ?p ?o }");
            System.out.println(Boolean.toString(result.next()));
        } catch (DatabaseException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    
    /** Disable constructor. */
    private Application() {
    }
}
