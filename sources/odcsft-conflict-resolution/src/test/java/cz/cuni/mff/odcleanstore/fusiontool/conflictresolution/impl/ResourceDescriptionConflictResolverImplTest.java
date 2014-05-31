package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import com.google.common.base.Supplier;
import com.google.common.collect.*;
import cz.cuni.mff.odcleanstore.conflictresolution.*;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ResolutionFunctionNotRegisteredException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictResolutionPolicyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolutionStrategyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.EmptyMetadataModel;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.util.Pair;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.util.*;

import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ODCSFTTestUtils.createHttpStatement;
import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ODCSFTTestUtils.createHttpUri;
import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ResolvedStatementMatchesSources.resolvedStatementMatchesSources;
import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ResolvedStatementMatchesStatement.resolvedStatementMatchesStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResourceDescriptionConflictResolverImplTest {
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
    private static final Supplier<List<MockResolvedStatement>> LIST_SUPPLIER = new Supplier<List<MockResolvedStatement>>() {
        @Override
        public List<MockResolvedStatement> get() {
            return new ArrayList<>();
        }
    };

    private UriMappingImpl uriMapping;

    @Before
    public void setUp() throws Exception {
        // init URI mapping
        // map sa, sb -> sx; pa, pb -> px; oa, ob -> ox; ga, gb -> gx
        UriMappingImpl uriMappingImpl = new UriMappingImpl(ImmutableSet.of(
                createHttpUri("sx").toString(),
                createHttpUri("px").toString(),
                createHttpUri("ox").toString(),
                createHttpUri("gx").toString()));
        uriMappingImpl.addLink(createHttpUri("sa").toString(), createHttpUri("sx").toString());
        uriMappingImpl.addLink(createHttpUri("sb").toString(), createHttpUri("sx").toString());
        uriMappingImpl.addLink(createHttpUri("pa").toString(), createHttpUri("px").toString());
        uriMappingImpl.addLink(createHttpUri("pb").toString(), createHttpUri("px").toString());
        uriMappingImpl.addLink(createHttpUri("oa").toString(), createHttpUri("ox").toString());
        uriMappingImpl.addLink(createHttpUri("ob").toString(), createHttpUri("ox").toString());
        uriMappingImpl.addLink(createHttpUri("ga").toString(), createHttpUri("gx").toString());
        uriMappingImpl.addLink(createHttpUri("gb").toString(), createHttpUri("gx").toString());
        this.uriMapping = uriMappingImpl;
    }

    @Test
    public void splitsToCorrectConflictClusters() throws Exception {
        // Arrange
        Resource resource = createHttpUri("s4");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("s4", "p1", "o1", "g1"),
                createHttpStatement("s4", "p1", "o1", "g2"),
                createHttpStatement("s4", "p2", "o1", "g1"),
                createHttpStatement("s4", "p2", "o2", "g2"),
                createHttpStatement("s4", "p3", "o1", "g1"),
                createHttpStatement("s4", "p3", "o2", "g2"),
                createHttpStatement("s4", "p3", "o2", "g2")
        );
        ResourceDescriptionConflictResolver resolver = createResolver();

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Collection<Collection<MockResolvedStatement>> conflictClusters = getConflictClusters(result);
        for (Collection<MockResolvedStatement> conflictCluster : conflictClusters) {
            MockResolvedStatement first = conflictCluster.iterator().next();
            for (MockResolvedStatement mockResolvedStatement : conflictCluster) {
                assertThat(mockResolvedStatement.getStatement().getSubject(), is(first.getStatement().getSubject()));
                assertThat(mockResolvedStatement.getStatement().getPredicate(), is(first.getStatement().getPredicate()));
            }
        }
    }

    @Test
    public void processesAllConflictClusters() throws Exception {
        // Arrange
        Resource resource = createHttpUri("s4");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("s4", "p1", "o1", "g1"),
                createHttpStatement("s4", "p1", "o1", "g2"),
                createHttpStatement("s4", "p2", "o1", "g1"),
                createHttpStatement("s4", "p2", "o2", "g2"),
                createHttpStatement("s4", "p3", "o1", "g1"),
                createHttpStatement("s4", "p3", "o2", "g2"),
                createHttpStatement("s4", "p3", "o2", "g2")
        );
        ResourceDescriptionConflictResolver resolver = createResolver();

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Set<Pair<Resource, URI>> expectedClusterSubjectPredicates = new HashSet<>();
        expectedClusterSubjectPredicates.add(Pair.create((Resource) createHttpUri("s4"), createHttpUri("p1")));
        expectedClusterSubjectPredicates.add(Pair.create((Resource) createHttpUri("s4"), createHttpUri("p2")));
        expectedClusterSubjectPredicates.add(Pair.create((Resource) createHttpUri("s4"), createHttpUri("p3")));

        Set<Pair<Resource, URI>> actualClusterSubjectPredicates = new HashSet<>();
        for (ResolvedStatement resolvedStatement : result) {
            actualClusterSubjectPredicates.add(Pair.create(
                    resolvedStatement.getStatement().getSubject(),
                    resolvedStatement.getStatement().getPredicate()));
        }

        assertThat(actualClusterSubjectPredicates, is(expectedClusterSubjectPredicates));
    }

    @Test
    public void processesAllConflictClustersWithUriMapping() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sa");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "pa", "oa", "g1"),
                createHttpStatement("sb", "pb", "ob", "g1"),
                createHttpStatement("sa", "p1", "oa", "ga")
        );
        ResourceDescriptionConflictResolver resolver = createResolver();

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Set<Pair<Resource, URI>> expectedClusterSubjectPredicates = new HashSet<>();
        expectedClusterSubjectPredicates.add(Pair.create((Resource) createHttpUri("sx"), createHttpUri("px")));
        expectedClusterSubjectPredicates.add(Pair.create((Resource) createHttpUri("sx"), createHttpUri("p1")));

        Set<Pair<Resource, URI>> actualClusterSubjectPredicates = new HashSet<>();
        for (ResolvedStatement resolvedStatement : result) {
            actualClusterSubjectPredicates.add(Pair.create(
                    resolvedStatement.getStatement().getSubject(),
                    resolvedStatement.getStatement().getPredicate()));
        }

        assertThat(actualClusterSubjectPredicates, is(expectedClusterSubjectPredicates));
    }


    @Test
    public void resolvesOnlySpecifiedResource() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        ResourceDescriptionConflictResolver resolver = createResolver();
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("s1", "p1", "o1", "g1"),
                createHttpStatement("s2", "p2", "o1", "g1"),
                createHttpStatement("sa", "p1", "oa", "g1"),
                createHttpStatement("sb", "p2", "oa", "g1"),
                createHttpStatement("oa", "pa", "o1"),
                createHttpStatement("o1", "p1", "oa")
        );

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        for (ResolvedStatement resolvedStatement : result) {
            assertThat(resolvedStatement.getStatement().getSubject(), is(resource));
        }
        assertThat(result, hasSize(2));
    }

    @Test
    public void removesDuplicates() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        ResourceDescriptionConflictResolver resolver = createResolver();
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "pa", "oa", "g1"),
                createHttpStatement("sb", "pb", "ob", "g1"),
                createHttpStatement("sa", "p1", "oa", "ga"),
                createHttpStatement("sx", "p1", "o1", "g1"),
                createHttpStatement("sx", "p1", "o1", "g1"),
                createHttpStatement("sx", "p1", "o1", "g2")
        );

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        List<ResolvedStatement> expectedStatements = ImmutableList.of(
                (ResolvedStatement) new ResolvedStatementImpl(createHttpStatement("sx", "px", "ox"), 0, ImmutableSet.of((Resource) createHttpUri("g1"))),
                new ResolvedStatementImpl(createHttpStatement("sx", "p1", "ox"), 0, ImmutableSet.of((Resource) createHttpUri("ga"))),
                new ResolvedStatementImpl(createHttpStatement("sx", "p1", "o1"), 0, ImmutableSet.of((Resource) createHttpUri("g1"))),
                new ResolvedStatementImpl(createHttpStatement("sx", "p1", "o1"), 0, ImmutableSet.of((Resource) createHttpUri("g2"))));

        assertThat(result, hasSize(expectedStatements.size()));
        for (ResolvedStatement expectedStatement : expectedStatements) {
            assertThat(result, hasItem(allOf(
                    resolvedStatementMatchesStatement(expectedStatement.getStatement()),
                    resolvedStatementMatchesSources((Set<Resource>) expectedStatement.getSourceGraphNames()))));
        }
    }

    @Test
    public void mapsUris() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        ResourceDescriptionConflictResolver resolver = createResolver();
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "pa", "oa", "g1"),
                createHttpStatement("sb", "pb", "ob", "g2"),
                createHttpStatement("sa", "p1", "oa", "ga")
        );

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        List<ResolvedStatement> expectedStatements = ImmutableList.of(
                (ResolvedStatement) new ResolvedStatementImpl(createHttpStatement("sx", "px", "ox"), 0, ImmutableSet.of((Resource) createHttpUri("g1"))),
                new ResolvedStatementImpl(createHttpStatement("sx", "px", "ox"), 0, ImmutableSet.of((Resource) createHttpUri("g2"))),
                new ResolvedStatementImpl(createHttpStatement("sx", "p1", "ox"), 0, ImmutableSet.of((Resource) createHttpUri("ga"))));

        assertThat(result, hasSize(expectedStatements.size()));
        for (ResolvedStatement expectedStatement : expectedStatements) {
            assertThat(result, hasItem(allOf(
                    resolvedStatementMatchesStatement(expectedStatement.getStatement()),
                    resolvedStatementMatchesSources((Set<Resource>) expectedStatement.getSourceGraphNames()))));
        }
    }

    @Test
    public void usesCorrectResolutionStrategy() throws Exception {
        // Arrange
        Resource resource = createHttpUri("s1");
        ConflictResolutionPolicyImpl conflictResolutionPolicy = new ConflictResolutionPolicyImpl();
        ResolutionStrategy defaultResolutionStrategy = new ResolutionStrategyImpl("DEFAULT", EnumCardinality.SINGLEVALUED, EnumAggregationErrorStrategy.IGNORE);
        ResolutionStrategy pbResolutionStrategy = new ResolutionStrategyImpl("PB");
        ResolutionStrategyImpl p1ResolutionStrategy = new ResolutionStrategyImpl("P1");

        conflictResolutionPolicy.setDefaultResolutionStrategy(defaultResolutionStrategy);
        conflictResolutionPolicy.setPropertyResolutionStrategy(ImmutableMap.of(
                createHttpUri("pb"), pbResolutionStrategy,
                createHttpUri("p1"), p1ResolutionStrategy));
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy);
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("s1", "pa", "o1", "g1"),
                createHttpStatement("s1", "p1", "o1", "g2"),
                createHttpStatement("s1", "p2", "o1", "g3")
        );

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Map<URI, ResolutionStrategy> expectedResolutionStrategies = new HashMap<>();
        expectedResolutionStrategies.put(createHttpUri("px"), new ResolutionStrategyImpl("PB", EnumCardinality.SINGLEVALUED, EnumAggregationErrorStrategy.IGNORE));
        expectedResolutionStrategies.put(createHttpUri("p1"), new ResolutionStrategyImpl("P1", EnumCardinality.SINGLEVALUED, EnumAggregationErrorStrategy.IGNORE));
        expectedResolutionStrategies.put(createHttpUri("p2"), new ResolutionStrategyImpl("DEFAULT", EnumCardinality.SINGLEVALUED, EnumAggregationErrorStrategy.IGNORE));

        Map<URI, ResolutionStrategy> actualResolutionStrategies = new HashMap<>();
        for (ResolvedStatement resolvedStatement : result) {
            MockResolvedStatement mockResolvedStatement = (MockResolvedStatement) resolvedStatement;
            actualResolutionStrategies.put(resolvedStatement.getStatement().getPredicate(), mockResolvedStatement.getResolutionStrategy());
        }
        assertThat(actualResolutionStrategies, is(expectedResolutionStrategies));
    }

    @Test
    public void resolvesEmptyInput() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        ResourceDescriptionConflictResolver resolver = createResolver();
        Collection<Statement> testInput = ImmutableList.of();

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        assertThat(result, empty());
    }

    @Test
    public void resolvesBlankNodes() throws Exception {
        // Arrange
        Resource resource = VF.createBNode("bnode1");
        ResourceDescriptionConflictResolver resolver = createResolver();
        Collection<Statement> testInput = ImmutableList.of(
                VF.createStatement(resource, createHttpUri("p1"), VF.createLiteral("a"), createHttpUri("g1")),
                VF.createStatement(resource, createHttpUri("p1"), resource, createHttpUri("g1"))
        );

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Collection<Statement> expectedStatements = ImmutableList.copyOf(testInput);
        assertThat(result, hasSize(expectedStatements.size()));
        for (Statement expectedStatement : expectedStatements) {
            assertThat(result, hasItem(resolvedStatementMatchesStatement(expectedStatement)));
        }
    }

    @Test
    public void resolvesStatementWithEmptyGraph() throws Exception {
        // Arrange
        Resource resource = createHttpUri("s");
        ResourceDescriptionConflictResolver resolver = createResolver();
        Statement inputStatement = createHttpStatement("s", "p", "o", null);
        Collection<Statement> testInput = ImmutableList.of(inputStatement);

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        ResolvedStatement resultStatement = Iterables.getOnlyElement(result);
        assertThat(resultStatement, resolvedStatementMatchesStatement(inputStatement));
    }

    private ResourceDescriptionConflictResolver createResolver() throws ResolutionFunctionNotRegisteredException {
        return createResolver(new ConflictResolutionPolicyImpl());
    }

    private ResourceDescriptionConflictResolver createResolver(ConflictResolutionPolicy conflictResolutionPolicy)
            throws ResolutionFunctionNotRegisteredException {
        ResolutionFunctionRegistry resolutionFunctionRegistry = mock(ResolutionFunctionRegistry.class);
        when(resolutionFunctionRegistry.get(anyString())).thenReturn(new MockResolutionFunction());
        return new ResourceDescriptionConflictResolverImpl(
                resolutionFunctionRegistry,
                conflictResolutionPolicy,
                uriMapping,
                new EmptyMetadataModel(),
                "http://cr/");
    }

    private Collection<Collection<MockResolvedStatement>> getConflictClusters(Collection<ResolvedStatement> resolvedStatements) {
        Multimap<Integer, MockResolvedStatement> result = Multimaps.newListMultimap(
                new HashMap<Integer, Collection<MockResolvedStatement>>(), LIST_SUPPLIER);

        for (ResolvedStatement resolvedStatement : resolvedStatements) {
            MockResolvedStatement mockResolvedStatement = (MockResolvedStatement) resolvedStatement;
            result.put(mockResolvedStatement.getConflictClusterNumber(), mockResolvedStatement);
        }

        return result.asMap().values();
    }

}