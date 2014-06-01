package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatementFactory;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import java.util.Iterator;

public class SubjectMappingIterator implements Iterator<ResolvedStatement> {
    private final Iterator<ResolvedStatement> wrappedIterator;
    private final Resource subject;
    private final ResolvedStatementFactory resolvedStatementFactory;

    public SubjectMappingIterator(Iterator<ResolvedStatement> wrappedIterator, Resource subject, ResolvedStatementFactory resolvedStatementFactory) {
        this.wrappedIterator = wrappedIterator;
        this.subject = subject;
        this.resolvedStatementFactory = resolvedStatementFactory;
    }

    @Override
    public boolean hasNext() {
        return wrappedIterator.hasNext();
    }

    @Override
    public ResolvedStatement next() {
        ResolvedStatement next = wrappedIterator.next();
        Statement nextStatement = next.getStatement();
        if (nextStatement.getSubject().equals(subject)) {
            return next;
        } else {
            return resolvedStatementFactory.create(
                    subject,
                    nextStatement.getPredicate(),
                    nextStatement.getObject(),
                    next.getQuality(),
                    next.getSourceGraphNames());
        }
    }

    @Override
    public void remove() {
        wrappedIterator.remove();
    }
}
