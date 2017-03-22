import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

    /*
    reading and writing operation simulating.
    reading is differ with writing. because reading might not be effected by the churn.
     */

public class FileActivity implements Runnable  {

    private MyDFileSystem fs;
    private int operation; // 0 ==read, 1==write
    private int size; // size of read or write
    private int offset;
    private String operaFile;
    private static int rwPayload[];
    private static int networkPayload[];
    static{
        rwPayload = new int[(int) Math.pow(2,25)];
        networkPayload = new int[(int) Math.pow(2,20)];
    }

//    private Payload rwPayload1;
//    private Payload networkPayload1;

    public FileActivity(MyDFileSystem f, String file, int o, int off, int s){
        fs = f;
        operation = o;
        size = s;
        operaFile = file;
        offset = off;
    }

    public void fileOpera(String file, int o, int s){
        operation = o;
        size = s;
        operaFile = file;
    }

    public void run(){
//        String fileName = "./fileSequence.txt";

        ArrayList<ArrayList<String>> partitionList = new ArrayList<ArrayList<String>>(); //segment list store all the segments
        ArrayList<String> lineList = new ArrayList<String>();

        FileReader file = null;

        try {
            file = new FileReader(operaFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        BufferedReader buffer = new BufferedReader(file);

        try {

            String line = null;
            line = buffer.readLine();
            Random r = new Random();
            int count = 0;
            String operaMap[] ={"read","write"};
            while ( line != null){

                ReentrantLock lock = fs.getLock();
                lock.lock();

                if (line.length() != 0){
                    int size = r.nextInt(61)+3;
                    int data[] = new int[3];
                    if(operation == 0){
                        fs.read(line,offset,size,data);
                    }else{
                        fs.write(line,offset,size,data);
                    }
                    count++;
                }
                lock.unlock();
                line = buffer.readLine(); //read the input file by line
            }
            System.out.printf("%d %s finished(offset %d)\n",count,operaMap[operation],offset);
            //if it is write operation, balance of Hash Algorithm should be calculated
            if(operation == 1) {
                double balance = fs.calcHashBalance();
                System.out.printf(" balance = %e ", balance);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return;
    }


    // read payload simulate
    public static void rPayload(int size){
        if (size > rwPayload.length){
            System.out.printf("error: invaild size %d\n",size);
            return;
        }
        for(int i = 0; i< size; i++){
            rwPayload[i] += 1;
        }
        return;
    }

    //write payload simulate
    public static void wPayload(int size){
        if (size > rwPayload.length){
            System.out.printf("error: invaild size %d\n",size);
            return;
        }
        for(int i = 0; i < size-1; i++){
            int low = rwPayload[i]&0xFFFF;
            int high = rwPayload[i]&0xFFFF0000;

            if(rwPayload[i] == rwPayload[i+1]){
                high >>= 16;
                high += 1;
                high <<= 16;
            }else{
                rwPayload[i] += 1;
            }
        }
        if(size > 3) {
            rwPayload[size - 1] = rwPayload[size - 2];
        }
    }

//    network payload simulate
    public static void netPayload(int ratio){
        int payload = networkPayload.length / ratio;

        for(int i = 0; i< payload; i++){
            networkPayload[i] += 1;
        }
        return;
    }

    private class Payload{
        private String payloadPath;
        private long size;

        public Payload(String pPath, long s){
            payloadPath = pPath;
            size = s;
        }
    }

}
