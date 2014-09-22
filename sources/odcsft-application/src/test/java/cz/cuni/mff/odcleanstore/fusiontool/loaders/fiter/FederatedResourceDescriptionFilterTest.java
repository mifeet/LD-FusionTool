package cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter;

import com.google.common.collect.ImmutableList;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FederatedResourceDescriptionFilterTest {
    private static final List<ResourceDescription> RESOURCE_DESCRIPTIONS = ImmutableList.of(
            mock(ResourceDescription.class),
            mock(ResourceDescription.class),
            mock(ResourceDescription.class),
            mock(ResourceDescription.class)
    );

    @Test
    public void acceptsAllForNoFilter() throws Exception {
        FederatedResourceDescriptionFilter filter = new FederatedResourceDescriptionFilter();

        assertTrue(filter.accept(RESOURCE_DESCRIPTIONS.get(0)));
        assertTrue(filter.accept(RESOURCE_DESCRIPTIONS.get(1)));
        assertTrue(filter.accept(RESOURCE_DESCRIPTIONS.get(2)));
    }

    @Test
    public void filtersByFilterForOneFilter() throws Exception {
        ResourceDescriptionFilter innerFilter = mock(ResourceDescriptionFilter.class);
        when(innerFilter.accept(RESOURCE_DESCRIPTIONS.get(0))).thenReturn(true);
        when(innerFilter.accept(RESOURCE_DESCRIPTIONS.get(1))).thenReturn(false);
        when(innerFilter.accept(RESOURCE_DESCRIPTIONS.get(2))).thenReturn(true);
        FederatedResourceDescriptionFilter filter = new FederatedResourceDescriptionFilter(innerFilter);

        assertTrue(filter.accept(RESOURCE_DESCRIPTIONS.get(0)));
        assertFalse(filter.accept(RESOURCE_DESCRIPTIONS.get(1)));
        assertTrue(filter.accept(RESOURCE_DESCRIPTIONS.get(2)));
    }

    @Test
    public void filtersByAnyFilterForMultipleFilters() throws Exception {
        ResourceDescriptionFilter innerFilter1 = mock(ResourceDescriptionFilter.class);
        when(innerFilter1.accept(RESOURCE_DESCRIPTIONS.get(0))).thenReturn(true);
        when(innerFilter1.accept(RESOURCE_DESCRIPTIONS.get(1))).thenReturn(false);
        when(innerFilter1.accept(RESOURCE_DESCRIPTIONS.get(2))).thenReturn(true);
        when(innerFilter1.accept(RESOURCE_DESCRIPTIONS.get(3))).thenReturn(true);

        ResourceDescriptionFilter innerFilter2 = mock(ResourceDescriptionFilter.class);
        when(innerFilter2.accept(RESOURCE_DESCRIPTIONS.get(0))).thenReturn(true);
        when(innerFilter2.accept(RESOURCE_DESCRIPTIONS.get(1))).thenReturn(true);
        when(innerFilter2.accept(RESOURCE_DESCRIPTIONS.get(2))).thenReturn(true);
        when(innerFilter2.accept(RESOURCE_DESCRIPTIONS.get(3))).thenReturn(false);


        FederatedResourceDescriptionFilter filter = new FederatedResourceDescriptionFilter(innerFilter1, innerFilter2);

        assertTrue(filter.accept(RESOURCE_DESCRIPTIONS.get(0)));
        assertFalse(filter.accept(RESOURCE_DESCRIPTIONS.get(1)));
        assertTrue(filter.accept(RESOURCE_DESCRIPTIONS.get(2)));
        assertFalse(filter.accept(RESOURCE_DESCRIPTIONS.get(3)));
    }
}