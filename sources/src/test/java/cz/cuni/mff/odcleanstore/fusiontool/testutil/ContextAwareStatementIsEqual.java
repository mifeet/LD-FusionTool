package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.openrdf.model.Statement;

/**
 * Tests {@link org.openrdf.model.Statement} for equality.
 * Unlike {@link org.openrdf.model.Statement}'s equal, matches also context, but only
 * if it is not null in the actual value. In other words, if actual statement has
 * a non null context, the expected statement must have the same context, otherwise
 * the context of expected statement can be arbitrary.
 */
public class ContextAwareStatementIsEqual extends BaseMatcher<Statement> {
    private final Statement expectedStatement;

    public ContextAwareStatementIsEqual(Statement expectedStatement) {
        this.expectedStatement = expectedStatement;
    }

    @Override
    public boolean matches(Object actualValue) {
        if (actualValue == null) {
            return expectedStatement == null;
        } else if (!(actualValue instanceof Statement)) {
            return false;
        }

        Statement actualStatement = (Statement) actualValue;
        boolean tripleEquals = expectedStatement.getObject().equals(actualStatement.getObject())
                && expectedStatement.getSubject().equals(actualStatement.getSubject())
                && expectedStatement.getPredicate().equals(actualStatement.getPredicate());
        if (!tripleEquals) {
            return false;
        }

        return expectedStatement.getContext() == null
                ? true
                : expectedStatement.getContext().equals(actualStatement.getContext());
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue(expectedStatement);
    }

    /**
     * Creates a matcher testing {@link org.openrdf.model.Statement}s for equality so that
     * subject, predicate, object must be equal, and context must be equal only if expected
     * value's context is not null.
     */
    @Factory
    public static Matcher<Statement> contextAwareStatementIsEqual(Statement expectedStatement) {
        return new ContextAwareStatementIsEqual(expectedStatement);
    }
}

