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
import cz.cuni.mff.odcleanstore.crbatch.ConfigConstants;
import cz.cuni.mff.odcleanstore.crbatch.ConnectionFactory;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;

/**
 * Loads subjects of triples to be processed and gives an iterator over them.
 * The iterator uses an open cursor in the database in the current implementation.
 * @TODO: another option would be to load subjects and keep them all in the memory or on the FS.
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

        public boolean hasNext() {
            return next != null;
        }

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
     * Variable ?{@value ConfigConstants#NG_CONSTRAINT_VAR} represents a relevant payload graph.
     * 
     * Must be formatted with arguments:
     * (1) named graph constraint pattern
     * (2) graph name prefix filter
     */
    private static final String SUBJECTS_QUERY = "SPARQL"
            + "\n SELECT DISTINCT ?" + VAR_PREFIX + "s"
            + "\n WHERE {"
            + "\n   %1$s"
            + "\n   ?" + ConfigConstants.NG_CONSTRAINT_VAR + " <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n   %2$s"
            + "\n   {"
            + "\n     GRAPH ?" + ConfigConstants.NG_CONSTRAINT_VAR + " {"
            + "\n       ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     }"
            + "\n   }"
            + "\n   UNION"
            + "\n   {"
            + "\n     ?" + ConfigConstants.NG_CONSTRAINT_VAR + " <" + ODCS.attachedGraph + "> ?" + VAR_PREFIX + "attachedGraph."
            + "\n     GRAPH ?" + VAR_PREFIX + "attachedGraph {"
            + "\n       ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     }"
            + "\n   }"
            + "\n }";
    
    private final String namedGraphConstraintPattern;

    public TripleSubjectsLoader(ConnectionFactory connFactory, String namedGraphConstraintPattern) {
        super(connFactory);
        this.namedGraphConstraintPattern = namedGraphConstraintPattern;
    }

    /**
     * Returns all subjects of triples in payload graphs matching the given named graph constraint pattern 
     * and in their attached graphs.
     * The iterator should be closed after it is no longer needed.
     * The current implementation returns distinct values.
     * @return iterator over subjects of relevant triples
     */
    public SubjectsIterator getTripleSubjectIterator() throws CRBatchException {
        long startTime = System.currentTimeMillis();
        
        String query = String.format(Locale.ROOT, SUBJECTS_QUERY,
                namedGraphConstraintPattern, LoaderUtils.getGraphPrefixFilter(ConfigConstants.NG_CONSTRAINT_VAR));
        SubjectsIterator result = new SubjectsIterator(query, getConnectionFactory());
        LOG.debug("CR-batch: Triple subjects iterator initialized in {} ms", System.currentTimeMillis() - startTime);
        return result;
    }
}
