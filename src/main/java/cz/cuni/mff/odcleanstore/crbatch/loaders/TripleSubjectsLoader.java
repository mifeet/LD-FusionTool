package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.sql.SQLException;
import java.util.Locale;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;

import cz.cuni.mff.odcleanstore.connection.VirtuosoConnectionWrapper;
import cz.cuni.mff.odcleanstore.connection.WrappedResultSet;
import cz.cuni.mff.odcleanstore.connection.exceptions.DatabaseException;
import cz.cuni.mff.odcleanstore.crbatch.ConnectionFactory;
import cz.cuni.mff.odcleanstore.crbatch.config.QueryConfig;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;

/**
 * Loads subjects of triples to be processed and gives an iterator over them.
 * The iterator uses an open cursor in the database in the current implementation.
 * @todo another option would be to load subjects and keep them all in the memory or on the FS.
 * @author Jan Michelfeit
 */
public class TripleSubjectsLoader extends DatabaseLoaderBase {
    private static final Logger LOG = LoggerFactory.getLogger(TripleSubjectsLoader.class);
    
    /**
     * Iterator over subjects of relevant triples.
     */
    public final class SubjectsIterator implements Closeable {
        private WrappedResultSet subjectsResultSet;
        private VirtuosoConnectionWrapper connection;
        private Node next = null;

        /**
         * Creates a new instance.
         * @param query query that retrieves subjects from the database; the query must return
         *      the subjects as the first variable in the results
         * @param connectionFactory connection factory
         * @throws CRBatchException error
         */
        /*package*/SubjectsIterator(String query, ConnectionFactory connectionFactory) throws CRBatchException {
            try {
                this.connection = connectionFactory.createConnection();
                this.subjectsResultSet = this.connection.executeSelect(query);
            } catch (DatabaseException e) {
                throw new CRBatchException(CRBatchErrorCodes.QUERY_TRIPLE_SUBJECTS, "Database error", e);
            }
            
            next = getNextResult();
        }

        private Node getNextResult() throws CRBatchException {
            final int subjectVarIndex = 1;
            Node result;
            try {
                if (subjectsResultSet.next()) {
                    result = subjectsResultSet.getNode(subjectVarIndex);
                } else {
                    result = null;
                    close();
                }
            } catch (SQLException e) {
                throw new CRBatchException(CRBatchErrorCodes.TRIPLE_SUBJECT_ITERATION,
                        "Database error while iterating over triple subjects.", e);
            }
            return result;
        }

        /**
         * Returns {@code true} if the iteration has more elements.
         * @return {@code true} if the iteration has more elements
         */
        public boolean hasNext() {
            return next != null;
        }

        /**
         * Returns the next element in the iteration.
         * @return the next element in the iteration
         * @throws CRBatchException error
         */
        public Node next() throws CRBatchException {
            if (subjectsResultSet == null) {
                throw new IllegalStateException("The iterator has been closed");
            }
            if (next == null) {
                throw new NoSuchElementException();
            }
            Node result = next;
            next = getNextResult();
            return result;
        }

        @Override
        public void close() {
            if (subjectsResultSet != null) {
                subjectsResultSet.closeQuietly();
                subjectsResultSet = null;
            }
            if (connection != null) {
                connection.closeQuietly();
                connection = null;
            }
        }
    }
    
    /**
     * SPARQL query that gets all distinct subjects of triples to be processed.
     * Variable {@link #ngRestrictionVar} represents a relevant payload graph.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     * (4) graph name prefix filter
     */
    private static final String SUBJECTS_QUERY = "SPARQL %1$s"
            + "\n SELECT DISTINCT ?" + VAR_PREFIX + "s"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n   %4$s"
            + "\n   {"
            + "\n     GRAPH ?%3$s {"
            + "\n       ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     }"
            + "\n   }"
            + "\n   UNION"
            + "\n   {"
            + "\n     ?%3$s <" + ODCS.attachedGraph + "> ?" + VAR_PREFIX + "attachedGraph."
            + "\n     GRAPH ?" + VAR_PREFIX + "attachedGraph {"
            + "\n       ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     }"
            + "\n   }"
            + "\n }";
    

    /**
     * Creates a new instance.
     * @param connectionFactory factory for database connection
     * @param queryConfig Settings for SPARQL queries  
     */
    public TripleSubjectsLoader(ConnectionFactory connectionFactory, QueryConfig queryConfig) {
        super(connectionFactory, queryConfig);
    }

    /**
     * Returns all subjects of triples in payload graphs matching the given named graph constraint pattern 
     * and in their attached graphs.
     * The iterator should be closed after it is no longer needed.
     * The current implementation returns distinct values.
     * @return iterator over subjects of relevant triples
     * @throws CRBatchException error
     */
    public SubjectsIterator getTripleSubjectIterator() throws CRBatchException {
        long startTime = System.currentTimeMillis();
        
        String query = String.format(Locale.ROOT, SUBJECTS_QUERY,
                getPrefixDecl(),
                queryConfig.getNamedGraphRestrictionPattern(),
                queryConfig.getNamedGraphRestrictionVar(),
                getGraphPrefixFilter());
        SubjectsIterator result = new SubjectsIterator(query, getConnectionFactory());
        LOG.debug("CR-batch: Triple subjects iterator initialized in {} ms", System.currentTimeMillis() - startTime);
        return result;
    }
}
