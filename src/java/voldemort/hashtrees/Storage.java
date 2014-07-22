package voldemort.hashtrees;

public interface Storage {

    String get(String key);

    void put(String key, String value);

    void remove(String key);
}
