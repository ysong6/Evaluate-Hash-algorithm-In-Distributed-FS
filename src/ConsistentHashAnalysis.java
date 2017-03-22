import java.io.*;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConsistentHashAnalysis {

    public static void main(String args[]) throws IOException, InterruptedException {

        String operaSequence = "./fileSequence.txt";
//        createPayloadFile(payloadPath, (long) Math.pow(2,20));
//        createAccSequence(operaSequence, 5000);
        String acc[] = new String[10];
        Pattern extraBlankP = Pattern.compile(" {2,}"); //del the extra blank
        long t1 = 0;
        long t2 = 0;
        long t3 = 0;
        long t4 = 0;
        int s = 0;
        MyDFileSystem dfSys = null;
        BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
        String d = "sss";


        int data[] = new int[2];
        try {

            String line = null;
            String strList[] = {""}; //in case input would be interrupted

            System.out.println("Please input node number, disk number and chunk size of the FS :");

            line = buffer.readLine(); //read all n
            if (line != null && line.length() != 0) {
                Matcher m1 = extraBlankP.matcher(line);
                String second = m1.replaceAll(" ");
                strList = second.split(" ");
                int nodeNum = Integer.valueOf(strList[0]);

                dfSys = new MyDFileSystem(nodeNum, Integer.valueOf(strList[1]), Integer.valueOf(strList[2]));
//                TraditionalHash tHash = new TraditionalHash(Integer.valueOf(strList[1]) );
            }

            while (s == 0) {
                System.out.printf("\n------------------------------------------------------\n");
                System.out.printf("Please choose a operation :\n 1. File operation\n 2. Trigger a event\n 3. set hash algorithm\n 4. calculate balance\n 5. calculate smoothness\n 6. summary of file system \n 8. exit\n$ ");
                line = buffer.readLine(); //read all n
                if (line != null && line.length() != 0) {
                    System.out.printf("-------------------------------------------------------\n");

                    int choice = Integer.valueOf(line);
                    switch (choice) {
                        case 1:
                            System.out.println("Please input operation(0(read)|1(write)), offset, size of read or write :");
                            line = buffer.readLine(); //read all n
                            if (line != null && line.length() != 0) {
                                Matcher m1 = extraBlankP.matcher(line);
                                String second = m1.replaceAll(" ");

                                strList = second.split(" ");
                                if (strList.length != 3) {
                                    System.out.println("Input is not valid, please input again.");
                                    continue;

                                }
                                FileActivity fa = new FileActivity(dfSys, operaSequence, Integer.valueOf(strList[0]), Integer.valueOf(strList[1]), Integer.valueOf(strList[2]));
//                                Thread f = new Thread(fa);
//                                f.start();
                                fa.run();
                            }
                            break;
                        case 2:
                            while (true) {
                                System.out.println("Please input event: 0: node insertion, 1: disk insertion, 2:disk recovery 3:node crash, 4:disk crash:");
                                int nodeId = 0;
                                int diskId = 0;
                                int operation = 0;

                                line = buffer.readLine(); //read all n
                                if (line != null && line.length() != 0) {
                                    Matcher m1 = extraBlankP.matcher(line);
                                    String second = m1.replaceAll(" ");
                                    strList = second.split(" ");
                                    operation = Integer.valueOf(strList[0]);
                                    switch (operation) {
                                        case 0:
                                            break;
                                        case 1:
                                        case 3:
                                            System.out.println("Please input nodes Number:");
                                            line = buffer.readLine(); //read all n
                                            if (line != null && line.length() != 0) {
                                                m1 = extraBlankP.matcher(line);
                                                second = m1.replaceAll(" ");
                                                strList = second.split(" ");
                                                if (strList.length != 1) {
                                                    System.out.println("Input is not valid, please input again.");
                                                    continue;
                                                }
                                                nodeId = Integer.valueOf(strList[0]);

                                                break;
                                            }
                                        case 2:
                                        case 4:
                                            System.out.println("Please input nodes Number and disk number:");
                                            line = buffer.readLine(); //read all n
                                            if (line != null && line.length() != 0) {
                                                m1 = extraBlankP.matcher(line);
                                                second = m1.replaceAll(" ");
                                                strList = second.split(" ");
                                                if (strList.length != 2) {
                                                    System.out.println("Input is not valid, please input again.");
                                                    continue;
                                                }
                                                nodeId = Integer.valueOf(strList[0]);
                                                diskId = Integer.valueOf(strList[1]);

                                                break;
                                            }
                                            break;
                                        default:
                                            break;
                                    }

                                    Monitor monitor;
                                    monitor = new Monitor(dfSys, operation, nodeId, diskId);
//                                    Thread m = new Thread(monitor);
//                                    m.start();
                                    monitor.run();
                                    break;

                                }

                            }
                            break;
                        case 3:
                            System.out.println("Please choice hash algorithm, 0: traditional hash, 1: consistent hash:");
                            line = buffer.readLine(); //read all n
                            if (line != null && line.length() != 0) {
                                Matcher m1 = extraBlankP.matcher(line);
                                String second = m1.replaceAll(" ");
                                strList = second.split(" ");
                                if (strList.length != 1) {
                                    System.out.println("Input is not valid, please input again.");
                                    continue;
                                }
                                int algorithm = Integer.valueOf(strList[0]);
                                dfSys.setAlgorithm(algorithm);
                            }
                            break;
                        case 4:
                            System.out.printf("balance is %e\n",dfSys.calcHashBalance());
                            break;
                        case 5:
                            System.out.printf("%d chunks were moved\n", dfSys.getMovCount());
                            break;
                        case 6:
                            dfSys.summary();
                            break;
                        case 8:
                            s = 1;
                            System.out.println("Thank you, bye.");
                            break;
                        default:
                            System.out.println("error: input error number");
                    }
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.printf("ntime = %d, rtime= %d, wtime = %d", t2 - t1, t3 - t2, t4 - t3);

        return;
    }

    private static void createPayloadFile(String fileName, long size) throws IOException {
        OutputStream outputFile = new FileOutputStream(fileName);

        for (long i = 0; i < size; i++) {
            String out = i + "";
            outputFile.write(out.getBytes());
            outputFile.write('\n');
        }
        outputFile.close();
        return;
    }

    //create input file by generate page number randomly
    private static void createAccSequence(String fileName, int loops) throws IOException {
        OutputStream outputFile = new FileOutputStream(fileName);
        char map[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
        String suffix[] = {".txt", ".mov", ".pdf", ".doc"};
        int len = 0;
        Random r = new Random();
        for (int i = 0; i < loops; i++) {
            StringBuilder path = new StringBuilder();
            int directory = r.nextInt(2) + 1; // directory level
            int id = 0;

            for (int d = 0; d < directory; d++) {
                path.append('/');

                len = r.nextInt(10) + 1; //directory length
                for (int j = 0; j < len; j++) {
                    id = r.nextInt(25);
                    path.append(map[id]);
                }
            }
            path.append('/');

            //file name
            len = r.nextInt(5) + 1; //directory length
            for (int j = 0; j < len; j++) {
                id = r.nextInt(26);
                path.append(map[id]);
            }

            id = r.nextInt(3);
            path.append(suffix[id]);
            path.append("\n");

            outputFile.write(path.toString().getBytes());
        }
        outputFile.close();
        return;
    }

    public static boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        return pattern.matcher(str).matches();
    }

}
