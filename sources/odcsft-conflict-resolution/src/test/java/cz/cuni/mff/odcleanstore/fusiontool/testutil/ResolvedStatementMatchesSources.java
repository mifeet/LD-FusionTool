package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.openrdf.model.Resource;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests {@link org.openrdf.model.Statement} for equality.
 * Unlike {@link org.openrdf.model.Statement}'s equal, matches also context, but only
 * if it is not null in the actual value. In other words, if actual statement has
 * a non null context, the expected statement must have the same context, otherwise
 * the context of expected statement can be arbitrary.
 */
public class ResolvedStatementMatchesSources extends BaseMatcher<ResolvedStatement> {
    private final Set<Resource> expectedSources;

    public ResolvedStatementMatchesSources(Set<Resource> expectedSources) {
        this.expectedSources = expectedSources;
    }

    @Override
    public boolean matches(Object actualValue) {
        if (actualValue == null) {
            return expectedSources == null;
        } else if (!(actualValue instanceof ResolvedStatement)) {
            return false;
        }

        ResolvedStatement actualResolvedStatement = (ResolvedStatement) actualValue;
        Collection<Resource> actualSources = actualResolvedStatement.getSourceGraphNames();
        HashSet<Resource> actualSourcesSet = new HashSet<>(actualSources);
        if (actualSources.size() != actualSourcesSet.size()) {
            return false; // sources are not unique
        }
        return actualSourcesSet.equals(expectedSources);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("resolved statement with sources ").appendValue(expectedSources);
    }

    /**
     * Creates a matcher testing {@link org.openrdf.model.Statement}s for equality so that
     * subject, predicate, object must be equal, and context must be equal only if expected
     * value's context is not null.
     */
    @Factory
    public static Matcher<ResolvedStatement> resolvedStatementMatchesSources(Set<Resource> sources) {
        return new ResolvedStatementMatchesSources(sources);
    }
}

