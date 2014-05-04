package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.util.Collection;
import java.util.Iterator;

/**
 * Utility methods for JUnit tests.
 */
public final class ODCSFTTestUtils {
    private static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();


    /** Hide constructor for a utility class. */
    private ODCSFTTestUtils() {
    }

    private static volatile long uriCounter = 0;

    /** Returns a URI unique within a test run. @return URI as a string */
    public static String getUniqueURIString() {
        uriCounter++;
        return "http://example.com/" + Long.toString(uriCounter);
    }
    
    /** Returns a URI unique within a test run. @return URI */
    public static URI getUniqueURI() {
        return VALUE_FACTORY.createURI(getUniqueURIString());
    }

    /** Resets the URI counter used by {@link #getUniqueURIString()}. */
    public static void resetURICounter() {
        uriCounter = 0;
    }

    /**
     * Create a new quad with the given subject, predicate and object with a unique named graph URI.
     * @param subjectURI subject URI
     * @param predicateURI predicate URI
     * @param objectURI object URI
     * @return quad
     */
    public static Statement createStatement(String subjectURI, String predicateURI, String objectURI) {
        return VALUE_FACTORY.createStatement(
                VALUE_FACTORY.createURI(subjectURI),
                VALUE_FACTORY.createURI(predicateURI),
                VALUE_FACTORY.createURI(objectURI),
                VALUE_FACTORY.createURI(getUniqueURIString()));
    }
    
    /**
     * Create a new quad with the given subject, predicate and object with a unique named graph URI.
     * @param subjectURI subject URI
     * @param predicateURI predicate URI
     * @param objectURI object URI
     * @return quad
     */
    public static Statement createStatement(URI subjectURI, URI predicateURI, URI objectURI) {
        return VALUE_FACTORY.createStatement(
                (subjectURI),
                (predicateURI),
                (objectURI),
                (getUniqueURI()));
    }
    
    /** Create a new unique quad. @return quad */
    public static Statement createStatement() {
        return VALUE_FACTORY.createStatement(
                VALUE_FACTORY.createURI(getUniqueURIString()),
                VALUE_FACTORY.createURI(getUniqueURIString()),
                VALUE_FACTORY.createURI(getUniqueURIString()),
                VALUE_FACTORY.createURI(getUniqueURIString()));
    }
    
    /**
     * Create a new quad with the given subject, predicate, object and named graph URI.
     * @param subjectURI subject URI
     * @param predicateURI predicate URI
     * @param objectURI object URI
     * @param namedGraphURI named graph URI
     * @return quad
     */
    public static Statement createStatement(String subjectURI, String predicateURI, String objectURI, String namedGraphURI) {
        return VALUE_FACTORY.createStatement(
                VALUE_FACTORY.createURI(subjectURI),
                VALUE_FACTORY.createURI(predicateURI),
                VALUE_FACTORY.createURI(objectURI),
                VALUE_FACTORY.createURI(namedGraphURI));
    }

    /**
     * Compare two triples for equality; null-proof.
     * @param statement1 a triple
     * @param statement2 a triple
     * @return true iff the two triples are equal
     */
    public static boolean statementsEqual(Statement statement1, Statement statement2) {
        if (statement1 == null || statement2 == null) {
            return statement1 == statement2;
        }
        return statement1.equals(statement2) 
                && ODCSUtils.nullProofEquals(statement1.getContext(), statement2.getContext());
    }

    /**
     * Returns true iff a quad equal to the given quad is contained in the given collection.
     * @param quad a quad
     * @param collection a collection of quads
     * @return true iff a quad equal to the given quad is contained in the given collection
     */
    public static boolean inCollection(Statement quad, Collection<Statement> collection) {
        for (Statement collectionQuad : collection) {
            if (statementsEqual(quad, collectionQuad)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Returns number of items the given iterator iterates over.
     * @param it iterator
     * @return number of items in the iteration
     */
    public static int countIteratorItems(Iterator<?> it) {
        int result = 0;
        while (it.hasNext()) {
            it.next();
            result++;
        }
        return result;
    }

    public static URI createHttpUri(String uriPart) {
        return VALUE_FACTORY.createURI("http://" + uriPart);
    }

    public static Statement createHttpStatement(String subjectUri, String predicateUri, String objectUri) {
        return createHttpStatement(subjectUri, predicateUri, objectUri, null);
    }

    public static Statement createHttpStatement(String subjectUri, String predicateUri, String objectUri, String contextUri) {
        return VALUE_FACTORY.createStatement(
                createHttpUri(subjectUri),
                createHttpUri(predicateUri),
                createHttpUri(objectUri),
                contextUri != null ? createHttpUri(contextUri) : null);
    }
}
