package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import com.google.common.base.Supplier;
import com.google.common.collect.*;
import cz.cuni.mff.odcleanstore.conflictresolution.*;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ResolutionFunctionNotRegisteredException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictResolutionPolicyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolutionStrategyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.EmptyMetadataModel;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.DummyFQualityCalculator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingImpl;
import cz.cuni.mff.odcleanstore.fusiontool.testutil.*;
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
    private static final URI RESOURCE_DESCRIPTION_URI = createHttpUri("resourceDescriptionProperty");

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
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, new MockNoneResolutionFunction());
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("s1", "pa", "o1", "g1"),
                createHttpStatement("s1", "p1", "o1", "g2"),
                createHttpStatement("s1", "p2", "o1", "g3")
        );

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Map<URI, ResolutionStrategy> expectedResolutionStrategies = new HashMap<>();
        expectedResolutionStrategies.put(createHttpUri("px"),
                new ResolutionStrategyImpl("PB", EnumCardinality.SINGLEVALUED, EnumAggregationErrorStrategy.IGNORE));
        expectedResolutionStrategies.put(createHttpUri("p1"),
                new ResolutionStrategyImpl("P1", EnumCardinality.SINGLEVALUED, EnumAggregationErrorStrategy.IGNORE));
        expectedResolutionStrategies.put(createHttpUri("p2"),
                new ResolutionStrategyImpl("DEFAULT", EnumCardinality.SINGLEVALUED, EnumAggregationErrorStrategy.IGNORE));

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

    @Test
    public void resolvesDependentPropertiesTogether() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "d1", "oa1", "good"),
                createHttpStatement("sa", "d2", "oa2", "bad"),
                createHttpStatement("sa", "d3", "oa3", "good"),
                createHttpStatement("sb", "d1", "ob1", "bad"),
                createHttpStatement("sb", "d2", "ob2", "good"),
                createHttpStatement("sb", "d3", "ob3", "bad"),
                createHttpStatement("sa", "d4", "oa4", "bad"),
                createHttpStatement("sb", "d4", "ob4", "good")
        );
        ConflictResolutionPolicy conflictResolutionPolicy = ConflictResolutionPolicyBuilder.newPolicy()
                .with(createHttpUri("d1"), resolutionStrategyWithDependsOn(createHttpUri("d3")))
                .with(createHttpUri("d2"), resolutionStrategyWithDependsOn(createHttpUri("d3")))
                .build();
        ResolutionFunction resolutionFunction = MockBestResolutionFunctionWithQuality.newResolutionFunction()
                .withQuality(createHttpUri("good"), 0.9)
                .withQuality(createHttpUri("bad"), 0.2);
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, resolutionFunction);

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Collection<Statement> expectedStatements = ImmutableList.of(
                createHttpStatement("sx", "d1", "oa1"),
                createHttpStatement("sx", "d2", "oa2"),
                createHttpStatement("sx", "d3", "oa3"),
                createHttpStatement("sx", "d4", "ob4"));
        assertThat(result, hasSize(expectedStatements.size()));
        for (Statement expectedStatement : expectedStatements) {
            assertThat(result, hasItem(resolvedStatementMatchesStatement(expectedStatement)));
        }
    }

    @Test
    public void mapsStatementsWithDependentPropertiesCorrectly() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "pa", "oa", "ga"),
                createHttpStatement("sb", "pb", "ob", "gb")
        );
        ConflictResolutionPolicy conflictResolutionPolicy = ConflictResolutionPolicyBuilder.newPolicy()
                .with(createHttpUri("dpa"), resolutionStrategyWithDependsOn(createHttpUri("pb")))
                .build();

        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, new MockNoneResolutionFunction());

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Collection<Statement> expectedStatements = ImmutableList.of(createHttpStatement("sx", "px", "ox"));
        assertThat(result, hasSize(expectedStatements.size()));
        for (Statement expectedStatement : expectedStatements) {
            assertThat(result, hasItem(resolvedStatementMatchesStatement(expectedStatement)));
        }
    }

    @Test
    public void resolvesDependentPropertyGroupsCorrectly() throws Exception {
        Resource resource = createHttpUri("sx");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "d1", "oa1", "good"),
                createHttpStatement("sa", "d2", "oa2", "good"),
                createHttpStatement("sa", "d3", "oa3", "bad"),
                createHttpStatement("sa", "d4", "oa4", "good"),

                createHttpStatement("sa", "d5", "oa5", "bad"),

                createHttpStatement("sb", "d1", "ob1", "bad"),
                createHttpStatement("sb", "d2", "ob2", "bad"),
                createHttpStatement("sb", "d3", "ob3", "good"),
                createHttpStatement("sb", "d4", "ob4", "bad"),

                createHttpStatement("sb", "d5", "ob5", "good")
        );
        ConflictResolutionPolicy conflictResolutionPolicy = ConflictResolutionPolicyBuilder.newPolicy()
                .with(createHttpUri("d1"), resolutionStrategyWithDependsOn(createHttpUri("d2")))
                .with(createHttpUri("d3"), resolutionStrategyWithDependsOn(createHttpUri("d4")))
                .with(createHttpUri("d4"), resolutionStrategyWithDependsOn(createHttpUri("d2")))
                .build();
        ResolutionFunction resolutionFunction = MockBestResolutionFunctionWithQuality.newResolutionFunction()
                .withQuality(createHttpUri("good"), 0.9)
                .withQuality(createHttpUri("bad"), 0.2);
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, resolutionFunction);

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Collection<Statement> expectedStatements = ImmutableList.of(
                createHttpStatement("sx", "d1", "oa1"),
                createHttpStatement("sx", "d2", "oa2"),
                createHttpStatement("sx", "d3", "oa3"),
                createHttpStatement("sx", "d4", "oa4"),
                createHttpStatement("sx", "d5", "ob5"));
        assertThat(result, hasSize(expectedStatements.size()));
        for (Statement expectedStatement : expectedStatements) {
            assertThat(result, hasItem(resolvedStatementMatchesStatement(expectedStatement)));
        }
    }

    @Test
    public void choosesBestStatementForDependentProperties() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "d1", "oa1", "good"),
                createHttpStatement("sa", "d1", "oa1", "bad"),
                createHttpStatement("sa", "d2", "oa2", "good"),
                createHttpStatement("sb", "d1", "ob1", "good"),
                createHttpStatement("sb", "d2", "ob2", "good"));
        ConflictResolutionPolicy conflictResolutionPolicy = ConflictResolutionPolicyBuilder.newPolicy()
                .with(createHttpUri("d1"), resolutionStrategyWithDependsOn(createHttpUri("d2")))
                .build();
        ResolutionFunction resolutionFunction = MockNoneResolutionFunctionWithQuality.newResolutionFunction()
                .withQuality(createHttpUri("good"), 0.9)
                .withQuality(createHttpUri("bad"), 0.2);
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, resolutionFunction);

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Collection<Statement> expectedStatements = ImmutableList.of(
                createHttpStatement("sx", "d1", "ob1"),
                createHttpStatement("sx", "d2", "ob2"));
        assertThat(result, hasSize(expectedStatements.size()));
        for (Statement expectedStatement : expectedStatements) {
            assertThat(result, hasItem(resolvedStatementMatchesStatement(expectedStatement)));
        }
    }

    @Test
    public void removesDuplicatesWithDependentProperties() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "d1", "oa", "ga"),
                createHttpStatement("sa", "d1", "oa", "ga"),
                createHttpStatement("sb", "d1", "ob", "gb"),
                createHttpStatement("sb", "d1", "ob", "gb"));
        ConflictResolutionPolicy conflictResolutionPolicy = ConflictResolutionPolicyBuilder.newPolicy()
                .with(createHttpUri("d1"), resolutionStrategyWithDependsOn(createHttpUri("d2")))
                .build();
        ResolutionFunction resolutionFunction = new MockNoneResolutionFunction();
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, resolutionFunction);

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Collection<Statement> expectedStatements = ImmutableList.of(createHttpStatement("sx", "d1", "ox"));
        assertThat(result, hasSize(expectedStatements.size()));
        for (Statement expectedStatement : expectedStatements) {
            assertThat(result, hasItem(resolvedStatementMatchesStatement(expectedStatement)));
        }
    }

    @Test
    public void resolvesWithDependentPropertiesCorrectlyWhenSomePropertiesMissing() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "d1", "oa1", "good"),
                createHttpStatement("sa", "d3", "oa3", "good"),
                createHttpStatement("sb", "d1", "ob1", "good"),
                createHttpStatement("sb", "d2", "ob2", "good"),
                createHttpStatement("sb", "d3", "ob3", "bad"),

                createHttpStatement("sa", "d4", "oa4", "good"),
                createHttpStatement("sb", "d5", "ob5", "bad"));
        ConflictResolutionPolicy conflictResolutionPolicy = ConflictResolutionPolicyBuilder.newPolicy()
                .with(createHttpUri("d1"), resolutionStrategyWithDependsOn(createHttpUri("d3")))
                .with(createHttpUri("d2"), resolutionStrategyWithDependsOn(createHttpUri("d3")))
                .with(createHttpUri("d4"), resolutionStrategyWithDependsOn(createHttpUri("d5")))
                .build();
        ResolutionFunction resolutionFunction = MockBestResolutionFunctionWithQuality.newResolutionFunction()
                .withQuality(createHttpUri("good"), 0.9)
                .withQuality(createHttpUri("bad"), 0.2);
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, resolutionFunction);

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Collection<Statement> expectedStatements = ImmutableList.of(
                createHttpStatement("sx", "d1", "ob1"),
                createHttpStatement("sx", "d2", "ob2"),
                createHttpStatement("sx", "d3", "ob3"),
                createHttpStatement("sx", "d4", "oa4"));
        assertThat(result, hasSize(expectedStatements.size()));
        for (Statement expectedStatement : expectedStatements) {
            assertThat(result, hasItem(resolvedStatementMatchesStatement(expectedStatement)));
        }
    }

    @Test
    public void resolvesIntraSourceConflictsWithDependentProperties() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "d1", "oa1", "bad"),
                createHttpStatement("sa", "d1", "oa1", "good"),
                createHttpStatement("sa", "d2", "oa2", "good"),
                createHttpStatement("sb", "d1", "ob1", "bad"),
                createHttpStatement("sb", "d1", "ob1", "good"),
                createHttpStatement("sb", "d2", "ob2", "bad"));
        ConflictResolutionPolicy conflictResolutionPolicy = ConflictResolutionPolicyBuilder.newPolicy()
                .with(createHttpUri("d1"), resolutionStrategyWithDependsOn(createHttpUri("d2")))
                .build();
        ResolutionFunction resolutionFunction = MockBestResolutionFunctionWithQuality.newResolutionFunction()
                .withQuality(createHttpUri("good"), 0.9)
                .withQuality(createHttpUri("bad"), 0.2);
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, resolutionFunction);

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Collection<Statement> expectedStatements = ImmutableList.of(
                createHttpStatement("sx", "d1", "oa1"),
                createHttpStatement("sx", "d2", "oa2"));
        assertThat(result, hasSize(expectedStatements.size()));
        for (Statement expectedStatement : expectedStatements) {
            assertThat(result, hasItem(resolvedStatementMatchesStatement(expectedStatement)));
        }
        for (ResolvedStatement resolvedStatement : result) {
            assertThat(resolvedStatement.getSourceGraphNames(), is((Collection<Resource>) Collections.singleton((Resource) createHttpUri("good"))));
        }
    }

    @Test
    public void usesCorrectConflictingStatementsWithDependentProperties() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "d1", "oa1", "good"),
                createHttpStatement("sa", "d1", "oa2", "bad"),
                createHttpStatement("sb", "d1", "ob1", "bad"),
                createHttpStatement("sb", "d1", "ob2", "bad"),
                createHttpStatement("s1", "d1", "o11", "bad"));
        ResolutionFunction resolutionFunction = MockBestResolutionFunctionWithQuality.newResolutionFunction()
                .withQuality(createHttpUri("good"), 0.9)
                .withQuality(createHttpUri("bad"), 0.2);
        ConflictResolutionPolicy conflictResolutionPolicy = ConflictResolutionPolicyBuilder.newPolicy()
                .with(createHttpUri("d1"), resolutionStrategyWithDependsOn(createHttpUri("d2")))
                .build();
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, new MockNoneResolutionFunction());

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        Map<URI, Collection<Statement>> expectedConflictingStatementsMap = new HashMap<>();
        expectedConflictingStatementsMap.put(createHttpUri("sx"), ImmutableList.of(
                createHttpStatement("sx", "d1", "oa1", "good"),
                createHttpStatement("sx", "d1", "oa2", "bad"),
                createHttpStatement("sx", "d1", "ob1", "bad"),
                createHttpStatement("sx", "d1", "ob2", "bad")));
        expectedConflictingStatementsMap.put(createHttpUri("s1"), ImmutableList.of(
                createHttpStatement("s1", "d1", "o11", "bad")));

        assertThat(result, hasSize(expectedConflictingStatementsMap.size()));
        for (ResolvedStatement resolvedStatement : result) {
            MockResolvedStatement mockResolvedStatement = (MockResolvedStatement) resolvedStatement;
            Collection<Statement> expectedConflictingStatements = expectedConflictingStatementsMap.get(resolvedStatement.getStatement().getSubject());
            assertThat(mockResolvedStatement.getConflictingStatements(), containsInAnyOrder(expectedConflictingStatements.toArray()));
        }
    }

    @Test
    public void contextConflictingStatementsAreMapped() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "p1", "oa", "good"),
                createHttpStatement("sb", "p1", "ob", "bad"));
        ResolutionFunction resolutionFunction = MockBestResolutionFunctionWithQuality.newResolutionFunction()
                .withQuality(createHttpUri("good"), 0.9)
                .withQuality(createHttpUri("bad"), 0.2);
        ConflictResolutionPolicy conflictResolutionPolicy = ConflictResolutionPolicyBuilder.newPolicy().build();
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, resolutionFunction);

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        MockResolvedStatement resolvedStatement = (MockResolvedStatement) Iterables.getOnlyElement(result);
        Collection<Statement> expectedConflictingStatements = ImmutableList.of(
                createHttpStatement("sx", "p1", "ox", "good"),
                createHttpStatement("sx", "p1", "ox", "bad"));
        Collection<Statement> actualConflictingStatements = resolvedStatement.getConflictingStatements();
        assertThat(actualConflictingStatements, containsInAnyOrder(expectedConflictingStatements.toArray()));
    }

    @Test
    public void contextConflictingStatementsAreMappedWithDependentProperties() throws Exception {
        // Arrange
        Resource resource = createHttpUri("sx");
        Collection<Statement> testInput = ImmutableList.of(
                createHttpStatement("sa", "d1", "oa", "good"),
                createHttpStatement("sb", "d1", "ob", "bad"),
                createHttpStatement("sa", "d2", "o1", "bad"));
        ResolutionFunction resolutionFunction = MockBestResolutionFunctionWithQuality.newResolutionFunction()
                .withQuality(createHttpUri("good"), 0.9)
                .withQuality(createHttpUri("bad"), 0.2);
        ConflictResolutionPolicy conflictResolutionPolicy = ConflictResolutionPolicyBuilder.newPolicy()
                .with(createHttpUri("d1"), resolutionStrategyWithDependsOn(createHttpUri("d2"))).build();
        ResourceDescriptionConflictResolver resolver = createResolver(conflictResolutionPolicy, resolutionFunction);

        // Act
        Collection<ResolvedStatement> result = resolver.resolveConflicts(new ResourceDescriptionImpl(resource, testInput));

        // Assert
        MockResolvedStatement resolvedStatement = (MockResolvedStatement) getFirstStatementWithProperty(result, createHttpUri("d1"));
        Collection<Statement> expectedConflictingStatements = ImmutableList.of(
                createHttpStatement("sx", "d1", "ox", "good"),
                createHttpStatement("sx", "d1", "ox", "bad"));
        Collection<Statement> actualConflictingStatements = resolvedStatement.getConflictingStatements();
        assertThat(actualConflictingStatements, containsInAnyOrder(expectedConflictingStatements.toArray()));
    }

    // FIXME: test for non-aggregable nested resource description statements

    private ResolvedStatement getFirstStatementWithProperty(Collection<ResolvedStatement> result, URI property) {
        for (ResolvedStatement resolvedStatement : result) {
            if (resolvedStatement.getStatement().getPredicate().equals(property)) {
                return resolvedStatement;
            }
        }
        return null;
    }

    private ResourceDescriptionConflictResolver createResolver() throws ResolutionFunctionNotRegisteredException {
        return createResolver(new ConflictResolutionPolicyImpl(), new MockNoneResolutionFunction());
    }

    private ResourceDescriptionConflictResolver createResolver(ConflictResolutionPolicy conflictResolutionPolicy, ResolutionFunction resolutionFunction)
            throws ResolutionFunctionNotRegisteredException {
        ResolutionFunctionRegistry resolutionFunctionRegistry = mock(ResolutionFunctionRegistry.class);
        when(resolutionFunctionRegistry.get(anyString())).thenReturn(resolutionFunction);
        return new ResourceDescriptionConflictResolverImpl(
                resolutionFunctionRegistry,
                conflictResolutionPolicy,
                uriMapping,
                new EmptyMetadataModel(),
                "http://cr/",
                new NestedResourceDescriptionQualityCalculatorImpl(new DummyFQualityCalculator()),
                Collections.singleton(RESOURCE_DESCRIPTION_URI));
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

    private ResolutionStrategyImpl resolutionStrategyWithDependsOn(URI dependsOn) {
        return new ResolutionStrategyImpl("XXX", EnumCardinality.MANYVALUED, EnumAggregationErrorStrategy.IGNORE,
                Collections.<String, String>emptyMap(), dependsOn);
    }
}