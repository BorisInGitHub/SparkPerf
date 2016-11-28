package common.perf;

import java.io.Serializable;

/**
 * A pair of two elements, used as an utility class.
 * Created by breynard
 * Date: 08/03/13
 * Time: 14:19
 */
public final class Pair<T1, T2> implements Serializable {
    private final T1 fst;
    private final T2 snd;

    /**
     * Constructs a new, immutable, {@link Pair} instance.
     *
     * @param fst First element of the pair.
     * @param snd Second element of the pair.
     */
    public Pair(T1 fst, T2 snd) {
        super();
        this.fst = fst;
        this.snd = snd;
    }

    /**
     * Returns the first element of the pair.
     */
    public T1 fst() {
        return fst;
    }

    /**
     * Returns the second element of the pair.
     */
    public T2 snd() {
        return snd;
    }

    @Override
    public String toString() {
        return "(" + fst + ", " + snd + ')';
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fst == null) ? 0 : fst.hashCode());
        result = prime * result + ((snd == null) ? 0 : snd.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Pair<?, ?> other = (Pair<?, ?>) obj;
        if (fst == null) {
            if (other.fst() != null) {
                return false;
            }
        } else if (!fst.equals(other.fst())) {
            return false;
        }
        if (snd == null) {
            if (other.snd() != null) {
                return false;
            }
        } else if (!snd.equals(other.snd())) {
            return false;
        }
        return true;
    }

}


