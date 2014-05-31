package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.URIMapping;
import cz.cuni.mff.odcleanstore.fusiontool.util.ConvertingIterator;
import cz.cuni.mff.odcleanstore.fusiontool.util.StatementMapper;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;

import java.util.Iterator;

public class StatementMappingIterator extends ConvertingIterator<Statement, Statement> {
    private final StatementMapper statementMapper;

    public StatementMappingIterator(Iterator<Statement> iterator,
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
    public Statement convert(Statement statement) {
        return statementMapper.mapStatement(statement);
    }
}
