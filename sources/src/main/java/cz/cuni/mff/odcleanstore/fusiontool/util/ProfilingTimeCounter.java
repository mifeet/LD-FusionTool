/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;

/**
 * Class providing methods for measurement of time spent by operations.
 * Counter are identified by values of enum &lt;E&gt;.
 * @param <E> enum for identification of counters that can measure time
 * @author Jan Michelfeit
 */
// CHECKSTYLE:OFF
public class ProfilingTimeCounter<E extends Enum<E>> {
// CHECKSTYLE:ON
    private static final long HOUR_MS = ODCSUtils.MILLISECONDS * ODCSUtils.TIME_UNIT_60 * ODCSUtils.TIME_UNIT_60;
    private final long[] lengths;
    private final long[] starts;

    /** 
     * Returns a new instance with counters enabled or disabled according to profilingOn parameter.
     * @param countersEnum enum class whose constants will identify each counter that can measure time
     * @param profilingOn whether the counters are enabled or disabled (and do not actually measure the time)
     * @param <E> enum for identification of counters
     * @return a new instance of {@link ProfilingTimeCounter}
     */
    public static <E extends Enum<E>> ProfilingTimeCounter<E> createInstance(Class<E> countersEnum, boolean profilingOn) {
        if (profilingOn) {
            return new ProfilingTimeCounter<E>(countersEnum);
        } else {
            return new DummyProfilingTimeCounter<E>(countersEnum);
        }
    }
    
    private ProfilingTimeCounter(Class<E> countersEnum) {
        starts = new long[countersEnum.getEnumConstants().length];
        lengths = new long[countersEnum.getEnumConstants().length];
    }
    
    /**
     * Starts a counter.
     * @param counterId counter to start
     */
    public void startCounter(E counterId) {
        starts[counterId.ordinal()] = System.currentTimeMillis();
    }
    
    /**
     * Stops a counter and remembers the period from when the counter was started.
     * @param counterId counter to stop
     */
    public void stopSetCounter(E counterId) {
        lengths[counterId.ordinal()] = System.currentTimeMillis() - starts[counterId.ordinal()];
    }
    
    /**
     * Stops a counter and adds the period from when the counter was started to previous measurements on this counter.
     * @param counterId counter to stop
     */
    public void stopAddCounter(E counterId) {
        lengths[counterId.ordinal()] += System.currentTimeMillis() - starts[counterId.ordinal()];
    }
    
    /**
     * Returns the time measured for this counter.
     * @param counterId counter 
     * @return time in milliseconds
     */
    public long getCounter(E counterId) {
        return lengths[counterId.ordinal()];
    }
    
    /**
     * Returns the time measured for this counter formatted in a human-readable string.
     * @param counterId counter 
     * @return formatted time
     */
    public String formatCounter(E counterId) {
        long timeInMs = lengths[counterId.ordinal()];
        DateFormat timeFormat = new SimpleDateFormat("mm:ss.SSS");
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return String.format(Locale.ROOT, "%d:%s (%d ms)\n", 
                timeInMs / HOUR_MS,
                timeFormat.format(new Date(timeInMs)),
                timeInMs);
    }
    
    /**
     * Child class which doesn't perform any measurements for use when profiling is turned off.
     * @param <E> see {@link ProfilingTimeCounter}
     */
    private static class DummyProfilingTimeCounter<E extends Enum<E>> extends ProfilingTimeCounter<E> {
        public DummyProfilingTimeCounter(Class<E> countersEnum) {
            super(countersEnum);
        }
        
        @Override
        public void startCounter(E counterId) {
            // do nothing 
        }
        
        @Override
        public void stopSetCounter(E counterId) {
            // do nothing 
        }
        
        @Override
        public void stopAddCounter(E counterId) {
            // do nothing 
        }
    }
    
}
