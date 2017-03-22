import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by vio on 16-11-21.
 */
public class ConsistentHash<T> {
    private final HashFunction hashFunction;
    //number of virtual nodes
    private final int numReplicas;
    private final SortedMap<Long, T> circle = new TreeMap<Long, T>();

    public ConsistentHash(HashFunction hashFunction, int numReplicas, Collection<T> nodes) {
        this.hashFunction = hashFunction;
        this.numReplicas = numReplicas;
        for(T node : nodes) {
            add(node);
        }
    }
    public void add(T node) {
        for(int i = 0; i < numReplicas; i++) {
            circle.put(hashFunction.hash(node.toString() + i), node);
        }
    }
    public void remove(T node) {
        for(int i = 0; i < numReplicas; i++) {

            circle.remove(hashFunction.hash(node.toString() + i));
        }
    }

    public T get(String key) {
        if(circle.isEmpty()) {
            return null;
        }
        long hash = hashFunction.hash(key);
        if(!circle.containsKey(hash)) {
            SortedMap<Long, T> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty()? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }
}
