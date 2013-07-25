/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import cz.cuni.mff.odcleanstore.shared.ODCSUtils;

/**
 * @author Jan Michelfeit
 */
public class ProfilingTimeCounter<E extends Enum<E>> {
    private static long HOUR_MS = ODCSUtils.MILLISECONDS * ODCSUtils.TIME_UNIT_60 * ODCSUtils.TIME_UNIT_60;
    private long[] lengths;
    private long[] starts;

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
    
    public void startCounter(E counterId) {
        starts[counterId.ordinal()] = System.currentTimeMillis();
    }
    
    public void stopSetCounter(E counterId) {
        lengths[counterId.ordinal()] = System.currentTimeMillis() - starts[counterId.ordinal()];
    }
    
    public void stopAddCounter(E counterId) {
        lengths[counterId.ordinal()] += System.currentTimeMillis() - starts[counterId.ordinal()];
    }
    
    public long getCounter(E counterId) {
        return lengths[counterId.ordinal()];
    }
    
    public String formatCounter(E counterId) {
        long timeInMs = lengths[counterId.ordinal()];
        DateFormat timeFormat = new SimpleDateFormat("mm:ss.SSS");
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return String.format(Locale.ROOT, "%d:%s (%d ms)\n", 
                timeInMs / HOUR_MS,
                timeFormat.format(new Date(timeInMs)),
                timeInMs);
    }
    
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
