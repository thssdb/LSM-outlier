import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpK {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;
        long startTime, endTime;
        /**
         WANHUA
         k = 50, r = 10, w = 20 min, s = 10 min

         */
        String[] datasetList = {"tianyuan", "wanhua", "jinfeng", "PAMAP2"};
        String curDataset = datasetList[3];
        double r = 0.4;

        Map<String, Integer> map = new HashMap<>();
        map.put("jinfeng", 1000000);
        map.put("tianyuan", 1000002);
        map.put("PAMAP2", 299999);
        map.put("wanhua", 1000000);

        int w,s;
//        int size = 999600;
        int size = 299400;
        int[] kList = new int[] {30, 40, 50, 60, 70, 80};


        Random random = new Random();
        int iterNum = 10;

//        LogWriter lw = new LogWriter("./result/ExpK/" + curDataset + "_res.dat");
//        lw.open();
//        lw.log("Size\tTsfile\tCPOD" + "\n");
//        for (int j= 0; j<kList.length; j++) {
//            int k = kList[j];
//            StringBuilder str = new StringBuilder();
//            /**
//            Tsfile
//             */
//            w=20;
//            s=10;
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
//            str.append(kList[j] + "\t");
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\t");
//            /**
//             CPOD
//             */
//            w=20*60;
//            s=10*60;
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
//
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
//            lw.log(str);

            /**
             * MCOD
             */
//            w=20*60;
//            s=10*60;
//            startTime = System.currentTimeMillis();
//            int minTime = random.nextInt(map.get(curDataset) - size) * 1000;
//            int maxTime = minTime + size * 1000;
//            String sql = "select mcod(s9, " +
//                    "'radius'='" + r + "', " +
//                    "'threshold'='" + k + "', " +
//                    "'window'='" + w + "', " +
//                    "'slide'='" + s + "') " +
//                    "from root.udf" + curDataset + ".d0 " +
//                    "where time>=" + minTime + " and time<" + maxTime;
//            System.out.println(sql);
//            result = session.executeQueryStatement(sql);
//            endTime = System.currentTimeMillis();
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0) + "\n");
//            lw.log(str);
//        }
        for (String tmpCurDataset:datasetList) {
            switch (tmpCurDataset){
                case "wanhua":
                case "tianyuan":
                    size = 999600;
                    r = 10;
                    break;
                case "jinfeng":
                    size = 999600;
                    r = 20;
                    break;
                case "PAMAP2":
                    size = 299400;
                    r = 0.4;
                    break;
                default:
                    break;
            }
            LogWriter lw = new LogWriter("./result/ExpR/" + tmpCurDataset + "_res.dat");
            lw.open();

            lw.log("Size\tMCOD" + "\n");
            for (int j = 0; j < kList.length; j++) {
                StringBuilder str = new StringBuilder();
                int k = kList[j];

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
