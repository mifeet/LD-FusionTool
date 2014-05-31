package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.conflictresolution.URIMapping;
import org.openrdf.model.*;

/**
 * TODO
 */
public class StatementMapper {
    private final URIMapping uriMapping;
    private final ValueFactory valueFactory;

    private boolean mapSubjects = true;
    private boolean mapPredicates = true;
    private boolean mapObjects = true;

    public StatementMapper(URIMapping uriMapping, ValueFactory valueFactory) {
        this.uriMapping = uriMapping;
        this.valueFactory = valueFactory;
    }

    public boolean mapsSubjects() {
        return mapSubjects;
    }

    public void setMapSubjects(boolean mapSubjects) {
        this.mapSubjects = mapSubjects;
    }

    public boolean mapsPredicates() {
        return mapPredicates;
    }

    public void setMapPredicates(boolean mapPredicates) {
        this.mapPredicates = mapPredicates;
    }

    public boolean mapsObjects() {
        return mapObjects;
    }

    public void setMapObjects(boolean mapObjects) {
        this.mapObjects = mapObjects;
    }

    public Statement mapStatement(Statement statement) {
        Resource subject = statement.getSubject();
        Resource mappedSubject = mapSubjects
                ? (Resource) mapUriNode(subject)
                : subject;
        URI predicate = statement.getPredicate();
        URI mappedPredicate = mapPredicates
                ? (URI) mapUriNode(predicate)
                : predicate;
        Value object = statement.getObject();
        Value mappedObject = mapObjects
                ? mapUriNode(object)
                : object;

        // Intentionally !=
        if (subject != mappedSubject
                || predicate != mappedPredicate
                || object != mappedObject) {
            statement = valueFactory.createStatement(
                    mappedSubject,
                    mappedPredicate,
                    mappedObject,
                    statement.getContext());
        }
        return statement;
    }

    /**
     * If mapping contains an URI to map for the passed {@link org.openrdf.model.URI} returns a {@link org.openrdf.model.URI} with the mapped URI, otherwise returns
     * <code>value</code>.
     * @param value a {@link org.openrdf.model.Value} to apply mapping to
     * @return node with applied URI mapping
     */
    protected Value mapUriNode(Value value) {
        if (value instanceof URI) {
            return uriMapping.mapURI((URI) value);
        }
        return value;
    }
}
