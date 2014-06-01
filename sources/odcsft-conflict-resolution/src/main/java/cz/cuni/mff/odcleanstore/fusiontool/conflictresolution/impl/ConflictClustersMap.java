package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;

import java.util.*;

// TODO: more efficient representation, e.g. using binary search
public class ConflictClustersMap {
    private static final Supplier<List<Statement>> LIST_SUPPLIER = new ListSupplier();

    private final Map<Resource, ListMultimap<URI, Statement>> canonicalSubjectPropertyMap;

    public static ConflictClustersMap fromCollection(Collection<Statement> statements, UriMapping uriMapping) {
        Map<Resource, ListMultimap<URI, Statement>> map = new HashMap<Resource, ListMultimap<URI, Statement>>();
        for (Statement statement : statements) {
            Resource canonicalSubject = uriMapping.mapResource(statement.getSubject());
            URI canonicalProperty = (URI) uriMapping.mapResource(statement.getPredicate());
            ListMultimap<URI, Statement> subjectMap = getOrCreateSubjectMultimap(canonicalSubject, map);
            subjectMap.put(canonicalProperty, statement);
        }
        return new ConflictClustersMap(map);
    }

    private static ListMultimap<URI, Statement> getOrCreateSubjectMultimap(Resource canonicalSubject, Map<Resource, ListMultimap<URI, Statement>> map) {
        ListMultimap<URI, Statement> subjectMultimap = map.get(canonicalSubject);
        if (subjectMultimap == null) {
            subjectMultimap = Multimaps.newListMultimap(new HashMap<URI, Collection<Statement>>(), LIST_SUPPLIER);
            map.put(canonicalSubject, subjectMultimap);
        }
        return subjectMultimap;
    }

    private ConflictClustersMap(Map<Resource, ListMultimap<URI, Statement>> canonicalSubjectPropertyMap) {
        this.canonicalSubjectPropertyMap = canonicalSubjectPropertyMap;
    }

    public Iterator<URI> listProperties(Resource canonicalSubject) {
        ListMultimap<URI, Statement> subjectSubMap = canonicalSubjectPropertyMap.get(canonicalSubject);
        if (subjectSubMap == null) {
            return Collections.emptyIterator();
        }
        return Iterators.unmodifiableIterator(subjectSubMap.keySet().iterator());
    }

    public List<Statement> getConflictClusterStatements(Resource canonicalSubject, URI canonicalProperty) {
        ListMultimap<URI, Statement> subjectSubMap = canonicalSubjectPropertyMap.get(canonicalSubject);
        if (subjectSubMap == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(subjectSubMap.get(canonicalProperty));
    }

    public Map<URI, List<Statement>> getResourceStatementsMap(Resource canonicalSubject) {
        ListMultimap<URI, Statement> subjectSubMap = canonicalSubjectPropertyMap.get(canonicalSubject);
        if (subjectSubMap == null) {
            return Collections.emptyMap();
        }
        return Multimaps.asMap(subjectSubMap);
    }


    private static class ListSupplier implements Supplier<List<Statement>> {
        @Override
        public List<Statement> get() {
            return new ArrayList<Statement>(5);
        }
    }
}
