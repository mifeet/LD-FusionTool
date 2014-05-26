package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import com.google.common.base.Objects;

public class Pair<F, S> {
    public final F first;
    public final S second;

    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Pair)) {
            return false;
        }
        Pair<?, ?> otherPair = (Pair<?, ?>) other;
        return Objects.equal(first, otherPair.first) && Objects.equal(second, otherPair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first, second);
    }

    public static <T1, T2> Pair<T1, T2> create(T1 first, T2 second) {
        return new Pair<T1, T2>(first, second);
    }
}