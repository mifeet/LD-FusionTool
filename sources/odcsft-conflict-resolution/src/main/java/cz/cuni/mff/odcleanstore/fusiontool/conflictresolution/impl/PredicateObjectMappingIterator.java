package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.URIMapping;
import cz.cuni.mff.odcleanstore.fusiontool.util.ConvertingIterator;
import cz.cuni.mff.odcleanstore.fusiontool.util.StatementMapper;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;

import java.util.Iterator;

public class PredicateObjectMappingIterator extends ConvertingIterator<Statement, Statement> {
    private final StatementMapper statementMapper;

    public PredicateObjectMappingIterator(Iterator<Statement> iterator, URIMapping uriMapping, ValueFactory valueFactory) {
        super(iterator);
        statementMapper = new StatementMapper(uriMapping, valueFactory);
        statementMapper.setMapSubjects(false);
        statementMapper.setMapPredicates(true);
        statementMapper.setMapObjects(true);
    }

    @Override
    public Statement convert(Statement statement) {
        return statementMapper.mapStatement(statement);
    }
}
