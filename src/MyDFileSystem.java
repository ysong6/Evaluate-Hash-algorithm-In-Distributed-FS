import javax.xml.soap.Node;
import java.text.Format;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class MyDFileSystem {
    private int nodesNum;
    private int disksNum;
    private int chunkNum; //the number of blocks per node
    private int chunkSize;
    private int diskSize;
    private ArrayList<NodeServer> nodesList; //simulating the node sever
    private HashMap<Integer,NodeServer> nodesMap; // control table for node? or block?
    private long timeStamp;

    private int curPos;
    private int rCount;
    private int wCount;
    private ReentrantLock lock;
    private int algorithm; // 0 = traditional hash,   1= consistent hash
    private int movCount; //the amount of data need to be relocated
    private int nodeCount;
    private final ConsistentHash<Integer>  cHashAlgorithm;
    private final HashFunction hashFunction;

    public MyDFileSystem(int nNum, int dNum, int cSize) {
        nodesNum = nNum;
        disksNum = dNum;
//        chunkNum = cNum; // number of chunk per disk
        nodesList = new ArrayList<NodeServer>(nNum); // node servers list
//        replicaNodeList = new ArrayList<NodeServer>(nNum); //replica node server 1:1
        chunkSize = cSize;
        diskSize = 300 * 1024;
        nodesMap = new HashMap<Integer, NodeServer>(nNum);
        ArrayList<Integer> list = new ArrayList<Integer>(nNum);
        for (int i = 0; i < nNum; i++) {
            NodeServer node = new NodeServer(i+1, diskSize, dNum, cSize, 1, 0);
            nodesList.add(node);
            nodesMap.put(node.nId,node);
            list.add(node.nId);
        }
        HashFunction hashFunc = new HashFunction();
        this.hashFunction = hashFunc;
        ConsistentHash<Integer> cHash = new ConsistentHash<Integer>(hashFunc, 1000, list);

        algorithm = 0;

        timeStamp = 0;
        curPos = 0;
        lock = new ReentrantLock();
        movCount = 0;
        nodeCount = nNum+1;
        cHashAlgorithm = cHash;
    }


    public boolean read(String path, int offset, int size, int data[]) {

        NodeServer dNode;
        int n = offset / chunkSize;
        Boolean res = true;

        NodeServer nServer = NodeServerLocate(path + n);
        if (nServer.state == false) {
            System.out.println("Error: node is down");
        }

        Chunk c = chunkLocate(path, offset, nServer);

        if (c == null || c.occupied == false) {
            System.out.printf("error:chunk in disk of Serv %d is not used or found", nServer.nId);
        } else {
            c.data += 1;//only for recording
        }
        FileActivity.rPayload(size);
        FileActivity.netPayload(1);

        rCount++;
        return res;
    }

    //assuming size is small than the chunk size
    public boolean write(String path, int offset, int size, int data[]) {

        boolean res = false;
        boolean isNew = false;
        String key = path + (offset / chunkSize);
        Chunk chunk;

        NodeServer nServer = NodeServerLocate(key);

        if (nServer.state == false) {
            System.out.println("Error: node is down");
            return res;
        }

        chunk = nServer.mapping.get(key);
        if (chunk == null) {//if chunk does not exist, create a new one
            chunk = allocateNewChunk(nServer, key);
            if (chunk == null) {
                System.out.printf("Error: node server %d is not exist for file %s\n", nServer.nId, key);
                return false;
            }
            isNew = true;
        }

        res = write2HW(chunk, size, data);
        res = w2replicate(nServer.replicaServer, chunk, size, data, isNew);
        wCount++;

        return res;
    }

    public boolean write2HW(Chunk c, int size, int data[]) {
        boolean res = true;
        c.data++;
        FileActivity.wPayload(size);

        return res;
    }

    public Boolean w2replicate(NodeServer reSever, Chunk chunk, int size, int data[], boolean isNew) {
//        NodeServer reSever = replicaNodeList.get(nodeId);
        Chunk rChu = reSever.disksList.get(chunk.id >> 24).chunksList[chunk.id & 0x00FFFFFF];
        if (isNew) {
            reSever.mapping.put(chunk.pathName, rChu);
            rChu.occupy(chunk.pathName, chunk.id);
            reSever.disksList.get(chunk.id >> 24).available--;
            reSever.available--;
        }

        write2HW(rChu, size, data);


        FileActivity.netPayload(1);
        return true;
    }


    //find the location of a chunk according to path+offset through hash algorithm
    public NodeServer NodeServerLocate(String key) {
        Integer nodeId = 0;
        NodeServer node;
        if(algorithm == 0) {
            nodeId = TraditionalHash.traditionalHash(nodesNum, key,hashFunction);
            node = nodesList.get(nodeId);
        }else{
            int n = cHashAlgorithm.get(key);
            node = nodesMap.get(n);
            if(node == null){
                System.out.printf("node %d can not be found",n);
            }
        }

        return node;
    }

    //locate a chunk according to the file's path, name and offset
    public Chunk chunkLocate(String pathName, int off, NodeServer ns) {
        int n = off / chunkSize;
        String key = pathName + n;

        Chunk chunk = ns.mapping.get(key);
        return chunk;
    }

    public void diskCrashing(int nsId, int dId) {
        //lock sth, I don't know now
//        nodesList[nsId].disksList[dId].state = false;
        NodeServer rServer = nodesList.get(nsId).replicaServer;
        nodesList.get(nsId).removeDisk(dId);

//        if (algorithm == 0) { // maybe not need to do this
//            updateLocation();
//        }

        recoverData4Disk(rServer, dId, false);
        rServer.removeDisk(dId);

        //release lock
    }

    public void nodeCrashing(int nsId) {
        //lock
        NodeServer rServer = nodesList.get(nsId).replicaServer;
        int id = rServer.nId;
        updateHashConstruction(false,id);
        nodesMap.remove(id);
        nodesList.get(nsId).release();//should do more than this
        nodesNum--;
        nodesList.remove(nsId);

        //update hash algorithm construction
        if (algorithm == 0) {
            updateLocation();
        }

        recoverData4NodeServ(rServer);
        rServer.release();//should do more than this

//        replicaNodeList.remove(nsId);

        //release lock
        return;
    }

    public void nodeInsert() {
        int n = nodeCount++;
        NodeServer node = new NodeServer(n, diskSize, disksNum, chunkSize, 0, 1);
        nodesList.add(node);
        nodesMap.put(n, node);
        updateHashConstruction(true, n);
        nodesNum++;

        if (algorithm == 0) {
            updateLocation();
        } else {
            updateLocation(); //might be different
        }

        return;
    }

    public void diskInsert(int nsId) {


        if (nsId > nodesList.size() - 1) {
            System.out.println("Error: this node server is not exist.\n");
            return;
        }
        NodeServer rServer = nodesList.get(nsId).replicaServer;
        nodesList.get(nsId).disksList.add(new Disk(diskSize, chunkSize, 0));
        rServer.disksList.add(new Disk(diskSize, chunkSize, 0));

        int dId = nodesList.get(nsId).disksList.size()-1;
        nodesList.get(nsId).insertDisk(dId);
        rServer.insertDisk(dId);

        return;
    }

    public void diskRecover(int nsId, int dId) {
        NodeServer rServer = nodesList.get(nsId).replicaServer;

        if (nsId > nodesList.size() - 1) {
            System.out.println("Error: this node server is not exist.\n");
            return;
        }
        if (dId > nodesList.get(nsId).disksList.size() - 1) {
            System.out.printf("Error: this disk %d in node %d did not exist before.\n", dId, nsId);
            return;
        } else {
            nodesList.get(nsId).insertDisk(dId);
            rServer.insertDisk(dId);
        }
        return;
    }

    public void recoverChunkOnNewLocation(NodeServer serv, Chunk c) {
        Chunk chunkNew;
        int data[] = new int[2];//fake data
        NodeServer rServer = serv.replicaServer;

        chunkNew = allocateNewChunk(serv, c.pathName);
        if (chunkNew == null) {
            System.out.printf("Error: node server %d is not exist for file %s\n", serv.nId, c.pathName);
            return;
        }
        chunkCopy(c, chunkNew);
        w2replicate(rServer, chunkNew, chunkSize, data, true);
        return;
    }

    //only recover chunk from replica, not care about clearing info in master node
    public void recoverData4Disk(NodeServer rServer, int dId, boolean isReHash) {
        int trunkId;
        int count = 0;
//        int newNsId = 0;
        int nsId = rServer.nId;
        NodeServer newServer = nodesMap.get(nsId);

//         = nodesList.get(nsId).replicaServer;

        Disk disk = rServer.disksList.get(dId);
        int occupiedNum = disk.chunksNum - disk.available;
        for (int i = 0; i < disk.chunksList.length; i++) {
            Chunk curCh = disk.chunksList[i];
            if (curCh.occupied == true) {
                if (isReHash) {
                    newServer = NodeServerLocate(curCh.pathName);
//                    if(newNsId != nsId){ //all
//                        movCount++;
//                    }
                } else {//if node is not down, mapping info should be deleted
                    ReleaseMapping4Chunk(nodesMap.get(nsId), curCh.pathName);//del info of this chunk in mapping
                }

                recoverChunkOnNewLocation(newServer, curCh);
                if (isReHash) {
                    releaseChunk(rServer, curCh);
                } else {
                    rServer.available++;
                    rServer.disksList.get(curCh.id >> 24).available++;
                    curCh.release();
                }
                count++;
                if (count == occupiedNum) {
                    break;
                }
            }

        }
        return;
    }

    public void recoverData4NodeServ(NodeServer rServer) {
//        NodeServer rServer = nodesList.get(nsId).replicaServer;

        for (int i = 0; i < rServer.disksList.size(); i++) {
            if (rServer.disksList.get(i).state == false) continue;
            recoverData4Disk(rServer, i, true);
        }
        return;
    }


    public void updateLocation() {
        movCount = 0;

        for (int i = 0; i < nodesList.size(); i++) {

            if (nodesList.get(i).state == false) continue;
            ArrayList<Chunk> cList = new ArrayList<Chunk>();

            Iterator<Map.Entry<String, Chunk>> iter = nodesList.get(i).mapping.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = iter.next();
                Chunk c = (Chunk) entry.getValue();
                NodeServer newNode = NodeServerLocate(c.pathName);
                if (newNode != nodesList.get(i)) {
                    movCount++;
                    recoverChunkOnNewLocation(newNode, c);
                    cList.add(c);
                }
            }
            Iterator<Chunk> iter1 = cList.iterator();
            while (iter1.hasNext()) {
                Chunk cur = iter1.next();
                releaseChunk(nodesList.get(i).replicaServer, cur);
                releaseChunk(nodesList.get(i), cur);
            }
        }
        return;
    }

    public void chunkCopy(Chunk src, Chunk de) {
        int data[] = new int[0];//fake data
        de.replicaId = src.replicaId;
        de.data = src.data;
        write2HW(de, chunkSize, data);
        return;
    }

    //release chunk
    public void releaseChunk(NodeServer node, Chunk c) {
        node.available++;
        node.disksList.get(c.id >> 24).available++;
        ReleaseMapping4Chunk(node, c.pathName);
        node.disksList.get(c.id >> 24).chunksList[c.id & 0x00FFFFFF].release();
    }

    public void ReleaseMapping4Chunk(NodeServer ns, String key) {
        ns.mapping.remove(key);
        return;
    }

    //only allocate a chunk in main node server
    public Chunk allocateNewChunk(NodeServer nServer, String key) {

        int chunkId = 0;
        int diskId = 0;
        int min = 0;
        int cur = 0;

        if (nServer.state == false) {
            System.out.printf("Error: node server %d is not exist for file %s\n", nServer.nId, key);
            return null;
        }

        for (int i = 0; i < nServer.disksList.size(); i++) {
            if (nServer.disksList.get(i).state == true) {
                if (min < nServer.disksList.get(i).available) {
                    min = nServer.disksList.get(i).available;
                    diskId = i;
                }
            }
        }
        Disk d = nServer.disksList.get(diskId);
        if (d.curAvail == -1) {
            System.out.printf("chunk is used out in disk %d node %d for %s\n", nServer.nId, diskId, key);
            return null;
        }
        chunkId = d.curAvail + (diskId << 24);
        d.chunksList[d.curAvail].occupy(key, chunkId);
        nServer.mapping.put(key, d.chunksList[d.curAvail]);
        d.available--;
        nServer.available--;

        //update available chunk pos
        cur = d.curAvail;
        d.curAvail = -1;

        if (d.available != 0) {
            for (int i = cur+1; i < d.chunksNum; i++) {
                if (!d.chunksList[i].occupied) {
                    d.curAvail = i;
                    break;
                }
            }
            if (d.curAvail == -1) {
                for (int i = 0; i < cur; i++) {
                    if (!d.chunksList[i].occupied) {
                        d.curAvail = i;
                        break;
                    }
                }
            }
        } else {
            System.out.printf("chunk is over in node %d disk %d for %s\n", nServer.nId, diskId, key);
        }

        return d.chunksList[cur];
    }

    //Distributed nodes situation


    public double calcHashBalance() {
        double balance = 0;
        int sum = 0;
        float average = 0;

        for (int i = 0; i < nodesList.size(); i++) {
            if (nodesList.get(i).state == true) {
                sum += nodesList.get(i).chunkNum - nodesList.get(i).available;
            }
        }
        average = sum / nodesNum;
        sum = 0;

        for (int i = 0; i < nodesList.size(); i++) {
            if (nodesList.get(i).state == true) {
                sum += Math.pow((nodesList.get(i).chunkNum - nodesList.get(i).available - average), 2);
            }
        }

        balance = Math.sqrt(sum / nodesNum);

        return balance;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public int getMovCount(){
        return movCount;
    }
    public void setAlgorithm(int algorithm) {
        this.algorithm = algorithm;
    }

    public void updateHashConstruction(boolean isAdd,int nodeId) {
        if(isAdd){
            cHashAlgorithm.add(nodeId);
        }else{
            cHashAlgorithm.remove(nodeId);
        }
        return;
    }

    public void summary(){
        StringBuilder info = new StringBuilder();
        Formatter infoF = new Formatter(info);
        String aMap[] = {"traditional hash","consistent hash"};
        int uChunk = 0;
        infoF.format("Algorithm is %s\n",aMap[algorithm]); // 0 = traditional hash,   1= consistent hash
        infoF.format("Chunk size is %s\n",chunkSize); // 0 = traditional hash,   1= consistent hash

        for(int i = 0; i< nodesList.size(); i++) {
            uChunk += (nodesList.get(i).chunkNum -nodesList.get(i).available);
        }
        infoF.format("%d used chunks\n",uChunk);

        infoF.format("%d servers:\n",nodesNum);
        for(int i = 0; i< nodesList.size(); i++){
            infoF.format("-------------------------------\n");
            infoF.format("Server %d :\n",nodesList.get(i).nId);
            collectInfo4NS(infoF, nodesList.get(i));

        }
        System.out.println(infoF.toString());
    }

    public void collectInfo4NS(Formatter info, NodeServer serv){

        info.format("  State: %s .\n",serv.state);
        info.format("  %d used chunk and %d available chunk left \n",serv.chunkNum - serv.available, serv.available );
        info.format("  %d mapping elements:\n",serv.mapping.size());
//        Iterator<Map.Entry<String,Chunk>> iter = serv.mapping.entrySet().iterator();
//        int count = 0;
//        while(iter.hasNext()){
//            Chunk c = iter.next().getValue();
//            info.format("    %d-Key:%s of chunk %d in disk %d \n", count++, c.pathName,c.id&0x00FFFFFF,c.id>>24);
//        }
//        info.format("-------------------------------\n");

        info.format("  %d disks:\n",serv.disksList.size());
        for(int j = 0; j < serv.disksList.size(); j++){
            info.format("  %dth:",j);
            collectInfo4Disk(info,serv.disksList.get(j));
        }
//        info.format("  %d chunk in this node server \n", serv.chunkNum);

//        private int chunkSize;

        return;
    }

    public void collectInfo4Disk(Formatter info, Disk disk){
        int count = 0;

        info.format(" %s ------------\n",disk.state);
        info.format("    %d used chunk and %d available chunk left \n",disk.chunksNum - disk.available, disk.available );
//        for(int i = 0; i < disk.chunksList.length; i ++){
//            Chunk c = disk.chunksList[i];
//            if(c.occupied == true ){
//                info.format("      %dth chunk: %s\n",i,c.pathName);
//                count++;
//            }
//
//        }
        info.format("    %d chunk in this disk \n", disk.chunksNum);
        info.format("    %d: current available chunk position\n",disk.curAvail);


//        if(count != (disk.chunksNum- disk.available))
//            info.format("----Error: chunk available count is not right\n");

//        private int size;

        return;
    }

    private class NodeServer {
        private Boolean state;
        private int nId;
        private int replicaNode;
        private int diskNum;
        private HashMap<String, Chunk> mapping;
        private int chunkSize;
        private ArrayList<Disk> disksList;
        private int chunkNum;
        private int available;
        private NodeServer replicaServer;

        public NodeServer(int id, int diskSize, int dNum, int cSize, int replicaId, int dReplicaNode) {
            state = true;
            this.nId = id;
            diskNum = dNum;
            disksList = new ArrayList<Disk>(dNum);
            for (int i = 0; i < dNum; i++) {
                disksList.add(new Disk(diskSize, cSize, replicaId));
            }
            replicaNode = dReplicaNode;
            mapping = new HashMap<String, Chunk>();
            available = dNum * (diskSize / cSize);
            chunkSize = cSize;
            chunkNum = dNum * (diskSize / cSize);
            replicaServer = new NodeServer(id, diskSize,  dNum,  cSize,  replicaId);
        }

        public NodeServer(int id, int diskSize, int dNum, int cSize, int replicaId) {
            this.nId = id;
            state = true;
            diskNum = dNum;
            disksList = new ArrayList<Disk>(dNum);
            for (int i = 0; i < dNum; i++) {
                disksList.add(new Disk(diskSize, cSize, replicaId));

            }
            mapping = new HashMap<String, Chunk>();
            replicaNode = 0;
            available = dNum * (diskSize / cSize);
            chunkSize = cSize;
            chunkNum = dNum * (diskSize / cSize);

        }

        public void release() {
            state = false;
            diskNum = -1;
            disksList = null;
            replicaNode = -1;
            mapping = null;
            available = -1;
            chunkSize = -1;
            chunkNum = -1;
        }

        public void initiate(int dNum, int diskSize, int cSize, int replicaId) {
            state = true;
            diskNum = dNum;
            disksList = new ArrayList<Disk>(dNum);
            for (int i = 0; i < dNum; i++) {
                disksList.add(new Disk(diskSize, cSize, replicaId));
            }
            replicaNode = -1;
            mapping = new HashMap<String, Chunk>();
            chunkSize = cSize;
            available = dNum * (diskSize / cSize);
            chunkNum = dNum * (diskSize / cSize);
        }

        public void removeDisk(int diskId) {
            Disk d = disksList.get(diskId);
            int ocuppiedNum = d.chunksNum - d.available;
            diskNum--;
            chunkNum -= d.chunksNum;
            available -= d.chunksNum;
            available += ocuppiedNum;
//            disksList.remove(diskId);
            d.clear();
        }

        public void insertDisk(int diskId) {
            Disk d = disksList.get(diskId);
            if(d.state == false) {
                d.initiate(diskSize, chunkSize, 1);
            }
            chunkNum += d.chunksNum;
            available += d.chunksNum;
            diskNum++;
        }
    }

    private class Disk {
        private boolean state;
        private int size;
        private int available;
        private int chunksNum;
        private int curAvail;
        private Chunk chunksList[];

        public Disk(int dSize, int cSize, int replicaId) {
            int cNum = dSize / cSize;
            state = true;
            available = cNum;
            chunksNum = cNum;
            chunksList = new Chunk[cNum];
            for (int i = 0; i < cNum; i++) {
                chunksList[i] = new Chunk(replicaId);
            }
            curAvail = 0;
            size = dSize; //300G
        }

        public void clear() {
            state = false;
            available = 0;
            chunksNum = 0;
            curAvail = -1;
            size = 0;
            for (int i = 0; i < chunksList.length; i++) {
                if (chunksList[i].occupied == true) {
                    chunksList[i].release();
                }
            }
        }

        public void initiate(int dSize, int cSize, int replicaId) {
            int cNum = dSize / cSize;
            state = true;
            available = cNum;
            chunksNum = cNum;
            chunksList = new Chunk[cNum];
            for (int i = 0; i < cNum; i++) {
                chunksList[i] = new Chunk(replicaId);
            }
            curAvail = 0;
            size = dSize; //300G
        }
    }

    private class Chunk {
        Boolean occupied;
        int replicaId;
        int id; //use id to verify the visit
        String pathName;
        int data;

        public Chunk(int rId) {
            occupied = false;
            replicaId = rId;
            pathName = null;
            data = 0;
            id = 0;
        }


        public void occupy(String fName, int id) {
            occupied = true;
            pathName = fName;
            this.id = id;
        }

        public void release() {
            occupied = false;
            data = -1;
            id = -1;
            pathName = null;
            replicaId = -1;
        }
    }


}
