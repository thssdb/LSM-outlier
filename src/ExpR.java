import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpR {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;
        long startTime, endTime;
        /**
         TIANYUAN
         bucket_width=2
         k = 50, r = 10, w = 20 min, s = 10 min
         WANHUA
         bucket_width=2
         k = 50, r = 10, w = 20 min, s = 10 min
         JINFENG
         bucket_width=5
         k = 50, r = 20, w = 20 min, s = 10 min
         PAMAP2
         buck_width=0.1
         k = 50, r = 0.4, w = 20 min, s = 10 min
         */
        String[] datasetList = {"tianyuan", "wanhua", "jinfeng", "PAMAP2"};
        String curDataset = datasetList[3];

        Map<String, Integer> map = new HashMap<>();
        map.put("jinfeng", 1000000);
        map.put("tianyuan", 1000002);
        map.put("PAMAP2", 299999);
        map.put("wanhua", 1000000);

        int k=50, w,s;

        int size = 0;
        double[] rList = new double[6];
        Random random = new Random();
//        switch (curDataset){
//            case "wanhua":
//            case "tianyuan":
//                size = 999600;
//                rList = new double[] {6,8,10,12,14,16};
//                break;
//            case "jinfeng":
//                size = 999600;
//                rList = new double[] {10, 15, 20, 25, 30, 35};
//                break;
//            case "PAMAP2":
//                size = 299400;
//                rList = new double[] {0.2, 0.3, 0.4, 0.5, 0.6, 0.7};
//                break;
//            default:
//                break;
//        }
//
//        LogWriter lw = new LogWriter("./result/ExpR/" + curDataset + "_res.dat");
//        lw.open();
//
//        lw.log("Size\tTsfile\tCPOD" + "\n");
//        Random random = new Random();
//        int iterNum;
//
//        for (int j= 0; j<rList.length; j++) {
//            StringBuilder str = new StringBuilder();
//            double r = rList[j];
//
////            /**
////             Tsfile
////             */
//            w=20;
//            s=10;
//            iterNum = 100;
//            startTime = System.currentTimeMillis();
//            for (int i = 0; i < iterNum; i++) {
//                int minTime = random.nextInt(map.get(curDataset) - size) * 1000;
//                int maxTime = minTime + size * 1000;
//                String sql = "select dodds(s9, " +
//                        "'r'='" + r + "', " +
//                        "'k'='" + k + "', " +
//                        "'w'='" + w + "', " +
//                        "'s'='" + s + "') " +
//                        "from root." + curDataset + ".d0 " +
//                        "where time>=" + minTime + " and time<" + maxTime;
//                result = session.executeQueryStatement(sql);
//            }
//            endTime = System.currentTimeMillis();
//            str.append(rList[j] + "\t");
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\t");
////            /**
////             CPOD
////             */
//            w=20*60;
//            s=10*60;
//            iterNum = 10;
//            startTime = System.currentTimeMillis();
//            for (int i = 0; i < iterNum; i++) {
//                int minTime = random.nextInt(map.get(curDataset) - size) * 1000;
//                int maxTime = minTime + size * 1000;
//                String sql = "select cpod(s9, " +
//                        "'r'='" + r + "', " +
//                        "'k'='" + k + "', " +
//                        "'w'='" + w + "', " +
//                        "'s'='" + s + "') " +
//                        "from root.udf" + curDataset + ".d0 " +
//                        "where time>=" + minTime + " and time<" + maxTime;
//                result = session.executeQueryStatement(sql);
//            }
//            endTime = System.currentTimeMillis();
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
//            lw.log(str);
//        }


        /**
         * MCOD
         */
        for (String tmpCurDataset:datasetList) {
            switch (tmpCurDataset){
                case "wanhua":
                case "tianyuan":
                    size = 999600;
                    rList = new double[] {6,8,10,12,14,16};
                    break;
                case "jinfeng":
                    size = 999600;
                    rList = new double[] {10, 15, 20, 25, 30, 35};
                    break;
                case "PAMAP2":
                    size = 299400;
                    rList = new double[] {0.2, 0.3, 0.4, 0.5, 0.6, 0.7};
                    break;
                default:
                    break;
            }
            LogWriter lw = new LogWriter("./result/ExpR/" + tmpCurDataset + "_res.dat");
            lw.open();

            lw.log("Size\tMCOD" + "\n");
            for (int j = 0; j < rList.length; j++) {
                StringBuilder str = new StringBuilder();
                double r = rList[j];

                w = 20 * 60;
                s = 10 * 60;
                startTime = System.currentTimeMillis();
                int minTime = random.nextInt(map.get(tmpCurDataset) - size) * 1000;
                int maxTime = minTime + size * 1000;
                String sql = "select mcod(s9, " +
                        "'radius'='" + r + "', " +
                        "'threshold'='" + k + "', " +
                        "'window'='" + w + "', " +
                        "'slide'='" + s + "') " +
                        "from root.udf" + tmpCurDataset + ".d0 " +
                        "where time>=" + minTime + " and time<" + maxTime;
                System.out.println(sql);
                result = session.executeQueryStatement(sql);
                endTime = System.currentTimeMillis();
                str.append(String.format("%.4f", (endTime - startTime) / 1000.0) + "\n");
                lw.log(str);
            }
        }

    }
}
