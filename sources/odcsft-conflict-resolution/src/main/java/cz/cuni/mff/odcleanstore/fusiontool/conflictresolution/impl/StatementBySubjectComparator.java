package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.CRUtils;
import org.openrdf.model.Statement;

import java.util.Comparator;

/**
*
*/
public class StatementBySubjectComparator implements Comparator<Statement> {
    private static final Comparator<Statement> INSTANCE = new StatementBySubjectComparator();

    public static Comparator<Statement> getInstance() {
        return INSTANCE;
    }

    @Override
    public int compare(Statement o1, Statement o2) {
        return CRUtils.compareValues(o1.getSubject(), o2.getSubject());
    }
}
