package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import org.openrdf.util.iterators.Iterators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ResolvedResult {
    private List<ResolvedStatement> result;

    public ResolvedResult() {
        this.result = new ArrayList<>();
    }

    public Collection<ResolvedStatement> getResult() {
        return result;
    }

    public void addToResult(Collection<ResolvedStatement> resolvedStatements) {
        result.addAll(resolvedStatements);
    }

    public void addToResult(Iterator<ResolvedStatement> resolvedStatements) {
        Iterators.addAll(resolvedStatements, result);
    }
}
