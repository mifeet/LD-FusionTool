package cz.cuni.mff.odcleanstore.fusiontool.urimapping;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class AlternativeURINavigatorTest {
    @Test
    public void listsAlternativesCorrectly() throws Exception {
        // Arrange
        URIMappingIterableImpl uriMapping = new URIMappingIterableImpl(ImmutableSet.of(
                "http://ax",
                "http://bx",
                "http://cx"
        ));
        uriMapping.addLink("http://a1", "http://ax");
        uriMapping.addLink("http://a2", "http://ax");
        uriMapping.addLink("http://a3", "http://a2");
        uriMapping.addLink("http://b1", "http://bx");
        uriMapping.addLink("http://cx", "http://cx");

        Set<String> setA = ImmutableSet.of("http://a1", "http://a2", "http://a3", "http://ax");
        Set<String> setB = ImmutableSet.of("http://b1", "http://bx");
        Set<String> setC = ImmutableSet.of("http://cx");
        Set<String> setD = ImmutableSet.of("http://dx");

        // Act
        AlternativeURINavigator alternativeURINavigator = new AlternativeURINavigator(uriMapping);

        // Assert
        assertThat(ImmutableSet.copyOf(alternativeURINavigator.listAlternativeUris("http://a1")), equalTo(setA));
        assertThat(ImmutableSet.copyOf(alternativeURINavigator.listAlternativeUris("http://a2")), equalTo(setA));
        assertThat(ImmutableSet.copyOf(alternativeURINavigator.listAlternativeUris("http://a3")), equalTo(setA));
        assertThat(ImmutableSet.copyOf(alternativeURINavigator.listAlternativeUris("http://ax")), equalTo(setA));
        assertThat(ImmutableSet.copyOf(alternativeURINavigator.listAlternativeUris("http://b1")), equalTo(setB));
        assertThat(ImmutableSet.copyOf(alternativeURINavigator.listAlternativeUris("http://bx")), equalTo(setB));
        assertThat(ImmutableSet.copyOf(alternativeURINavigator.listAlternativeUris("http://cx")), equalTo(setC));
        assertThat(ImmutableSet.copyOf(alternativeURINavigator.listAlternativeUris("http://dx")), equalTo(setD));
    }

    @Test
    public void hasAlternativeUrisWorksCorrectly() throws Exception {
        // Arrange
        URIMappingIterableImpl uriMapping = new URIMappingIterableImpl(ImmutableSet.of(
                "http://ax",
                "http://bx",
                "http://cx"
        ));
        uriMapping.addLink("http://a1", "http://ax");
        uriMapping.addLink("http://a2", "http://ax");
        uriMapping.addLink("http://a3", "http://a2");
        uriMapping.addLink("http://b1", "http://bx");
        uriMapping.addLink("http://cx", "http://cx");

        Set<String> setA = ImmutableSet.of("http://a1", "http://a2", "http://a3", "http://ax");
        Set<String> setB = ImmutableSet.of("http://b1", "http://bx");
        Set<String> setC = ImmutableSet.of("http://cx");
        Set<String> setD = ImmutableSet.of("http://dx");

        // Act
        AlternativeURINavigator alternativeURINavigator = new AlternativeURINavigator(uriMapping);

        // Assert
        assertThat(alternativeURINavigator.hasAlternativeUris("http://a1"), equalTo(true));
        assertThat(alternativeURINavigator.hasAlternativeUris("http://a2"), equalTo(true));
        assertThat(alternativeURINavigator.hasAlternativeUris("http://a3"), equalTo(true));
        assertThat(alternativeURINavigator.hasAlternativeUris("http://ax"), equalTo(true));
        assertThat(alternativeURINavigator.hasAlternativeUris("http://b1"), equalTo(true));
        assertThat(alternativeURINavigator.hasAlternativeUris("http://cx"), equalTo(false));
        assertThat(alternativeURINavigator.hasAlternativeUris("http://dx"), equalTo(false));
    }
}