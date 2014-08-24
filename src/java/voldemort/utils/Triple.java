package voldemort.utils;

public class Triple<F, S, T> {

    private final int PRIME = 31;
    private final F f;
    private final S s;
    private final T t;

    public static <F, S, T> Triple<F, S, T> create(F f, S s, T t) {
        return new Triple<F, S, T>(f, s, t);
    }

    public F getFirst() {
        return f;
    }

    public S getSecond() {
        return s;
    }

    public T getThird() {
        return t;
    }

    public Triple(F f, S s, T t) {
        this.f = f;
        this.s = s;
        this.t = t;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result += result * PRIME + ((f == null) ? 0 : f.hashCode());
        result += result * PRIME + ((s == null) ? 0 : s.hashCode());
        result += result * PRIME + ((t == null) ? 0 : t.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object that) {
        if(that == null)
            return false;
        if(!(that instanceof Triple<?, ?, ?>))
            return false;
        Triple<?, ?, ?> other = (Triple<?, ?, ?>) that;
        return f.equals(other.f) && s.equals(other.s) && t.equals(other.t);
    }
}
