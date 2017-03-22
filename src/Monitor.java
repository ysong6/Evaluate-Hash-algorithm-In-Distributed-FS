/*fail over: should relocate all data in the failing node and move them to the corresponding position.
During this operation, the whole system can not work. Should adding some operation to increase time to operation.
How to ? write a real file? or add some computation?
 */

//churn simulation? how to? periodically? two process? or just one? Should read it from a input file, so the situation can occur repeatedly.
//two kinds of event: adding and failing


import java.util.concurrent.locks.ReentrantLock;

public class Monitor implements Runnable {
    private MyDFileSystem fs;
    private int event; //0 == node insert, 1 == disk insert, 2 == node crash, 3==disk crash
    private int nodeSeverId;
    private int diskId;

    public Monitor(MyDFileSystem fs, int event, int nodesNum, int dId) {
        this.event = event;
        nodeSeverId = nodesNum;
        diskId = dId;
        this.fs = fs;
    }

    public void eventTrigger(int event, int nsId, int diskId) {
        this.event = event;
        nodeSeverId = nsId;
        this.diskId = diskId;
    }

    public void run() {
        ReentrantLock lock = fs.getLock();
        long t1 = 0;
        long t2 = 0;

        lock.lock();
        t1 = System.currentTimeMillis();

        switch (event) {
            case 0:
                fs.nodeInsert();
                break;
            case 1:
                fs.diskInsert(nodeSeverId);
                break;
            case 2:
                fs.diskRecover(nodeSeverId,diskId);
                break;
            case 3:
                fs.nodeCrashing(nodeSeverId);
                break;
            case 4:
                fs.diskCrashing(nodeSeverId, diskId);
                break;
            default:
                System.out.println("Error: invalid event number");
                break;

        }
        t2 = System.currentTimeMillis();

        lock.unlock();
        System.out.printf("event%d node %d slot %d last %d ms \n", event, nodeSeverId, diskId, t2 - t1);
        return;
    }

}
