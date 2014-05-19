package cz.cuni.mff.odcleanstore.fusiontool.urimapping;

import com.google.common.base.Stopwatch;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.URIMappingImpl;
import cz.cuni.mff.odcleanstore.fusiontool.testutil.Pair;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class UriMappingPerformanceTest {
    private static final int LINK_COUNT = 5000000;

    private static List<Pair<String, String>> links;

    @BeforeClass
    public static void setUpClass() throws Exception {
        final int randomRange = LINK_COUNT / 2;
        Random random = new Random(System.nanoTime());
        links = new ArrayList<Pair<String, String>>(LINK_COUNT);
        for (int i = 0; i < LINK_COUNT; i++) {
            links.add(Pair.create(
                    "n:" + (Integer.toHexString(random.nextInt(randomRange))),
                    "n:" + (Integer.toHexString(random.nextInt(randomRange)))));
        }
    }

    @Before
    public void setUp() throws Exception {
       System.gc();
    }

   // @Ignore
    @Test
    public void uriMappingImplTest() throws Exception {
        URIMappingImpl uriMapping = (URIMappingImpl) new URIMappingImpl();
        executeTest(uriMapping);
    }

    //@Ignore
    @Test
    public void uriMappingIterableImplTest() throws Exception {
        URIMappingImpl uriMapping = (URIMappingImpl) new URIMappingIterableImpl();
        executeTest(uriMapping);
    }

    private void executeTest(URIMappingImpl uriMapping) throws Exception {
        long memoryBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (Pair<String, String> link : links) {
            uriMapping.addLink(link.first, link.second);
        }
        stopwatch.stop();
        long memoryAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        System.out.printf("Uri mapping %s created with %,d sameAs links in %s\n", uriMapping.getClass().getSimpleName(), LINK_COUNT, stopwatch);
        System.out.printf("Memory before %,.2f MB, memory after %,.2f MB, difference %,.2f MB\n",
                memoryBefore / (double) ODCSFusionToolUtils.MB_BYTES,
                memoryAfter / (double) ODCSFusionToolUtils.MB_BYTES,
                (memoryAfter - memoryBefore) / (double) ODCSFusionToolUtils.MB_BYTES);
    }
}



