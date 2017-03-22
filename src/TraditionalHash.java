

public class TraditionalHash {

    public static int traditionalHash(int nodesNum, String key, HashFunction h){
        int res = 0;
        res = (int) (h.hash(key)&0x7FFFFFFF)%nodesNum;
//        res = (key.hashCode()&0x7FFFFFFF)% nodesNum;
        return res;
    }
}
