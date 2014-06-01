package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.URIMapping;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import cz.cuni.mff.odcleanstore.fusiontool.util.ConvertingIterator;
import cz.cuni.mff.odcleanstore.fusiontool.util.StatementMapper;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;

import java.util.Iterator;

public class ResolvedStatementMappingIterator extends ConvertingIterator<ResolvedStatement, ResolvedStatement> {
    private final StatementMapper statementMapper;

    public ResolvedStatementMappingIterator(Iterator<ResolvedStatement> iterator,
            URIMapping uriMapping,
            ValueFactory valueFactory,
            boolean mapSubjects,
            boolean mapPredicates,
            boolean mapObjects) {

        super(iterator);
        statementMapper = new StatementMapper(uriMapping, valueFactory);
        statementMapper.setMapSubjects(mapSubjects);
        statementMapper.setMapPredicates(mapPredicates);
        statementMapper.setMapObjects(mapObjects);
    }

    @Override
    public ResolvedStatement convert(ResolvedStatement resolvedStatement) {
        Statement mappedStatement = statementMapper.mapStatement(resolvedStatement.getStatement());
        if (mappedStatement == resolvedStatement.getStatement()) {
            return resolvedStatement;
        } else {
            return new ResolvedStatementImpl(mappedStatement, resolvedStatement.getQuality(), resolvedStatement.getSourceGraphNames());
        }
    }
}
