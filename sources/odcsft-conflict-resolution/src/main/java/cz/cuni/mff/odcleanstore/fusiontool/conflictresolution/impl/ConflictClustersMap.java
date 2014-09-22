package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.util.LDFusionToolCRUtils;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;

import java.util.*;

// TODO: more efficient representation, e.g. using binary search
public class ConflictClustersMap {
    /**
     * Map of canonical subject -> canonical property -> statements
     */
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
            subjectMultimap = LDFusionToolCRUtils.newStatementMultimap();
            map.put(canonicalSubject, subjectMultimap);
        }
        return subjectMultimap;
    }

    private ConflictClustersMap(Map<Resource, ListMultimap<URI, Statement>> canonicalSubjectPropertyMap) {
        this.canonicalSubjectPropertyMap = canonicalSubjectPropertyMap;
    }

    public Map<URI, List<Statement>> getResourceStatementsMap(Resource canonicalSubject) {
        ListMultimap<URI, Statement> subjectSubMap = canonicalSubjectPropertyMap.get(canonicalSubject);
        if (subjectSubMap == null) {
            return Collections.emptyMap();
        }
        return Multimaps.asMap(subjectSubMap);
    }

    public Map<URI, List<Statement>> getUnionStatementsMap(Set<Resource> canonicalSubjects) {
        ListMultimap<URI, Statement> result = LDFusionToolCRUtils.newStatementMultimap();
        for (Resource canonicalSubject : canonicalSubjects) {
            ListMultimap<URI, Statement> subjectSubMap = canonicalSubjectPropertyMap.get(canonicalSubject);
            if (subjectSubMap == null) {
                continue;
            }
            result.putAll(subjectSubMap);
        }
        return Multimaps.asMap(result);
    }

}
