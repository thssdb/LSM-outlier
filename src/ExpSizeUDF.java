import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpSizeUDF {

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;
        long startTime, endTime;
        /**
         TIANYUAN
            k = 50, r = 10, w = 20 min, s = 10 min
         WANHUA
            k = 50, r = 10, w = 20 min, s = 10 min
         JINFENG
            k = 50, r = 20, w = 20 min, s = 10 min
         PAMAP2
            k = 50, r = 0.4, w = 20 min, s = 10 min
         */

        Map<String, Integer> map = new HashMap<>();
        map.put("jinfeng", 1000000);
        map.put("tianyuan", 1000002);
        map.put("PAMAP2", 299999);
        map.put("wanhua", 1000000);

        String[] datasetList = {"tianyuan", "wanhua", "jinfeng", "PAMAP2"};
        String curDataset = datasetList[0];
        double r = 10;
        int k=50, w=20*60, s=10*60;

        int[] sizeList = new int[10];
        String[] outputSizeList;
        if (curDataset != "PAMAP2") {
            outputSizeList = new String[]{"0.1m", "0.2m", "0.3m", "0.4m", "0.5m",
                    "0.6m", "0.7m", "0.8m", "0.9m", "1m"};
            for (int i=0; i<10; i++){
                sizeList[i] = 99960 * (i+1);
            }
        }else{
            outputSizeList = new String[]{"30k", "60k", "90k", "120k", "150k",
                    "180k", "210k", "240k", "270k", "300k"};
            for (int i=0; i<10; i++){
                sizeList[i] = 29940 * (i+1);
            }
        }

        LogWriter lw = new LogWriter("./result/ExpSize/" + curDataset + "_res.dat");
        lw.open();

        lw.log("Size\tCPOD" + "\n");
        Random random = new Random();

        for (int j= 0; j<sizeList.length; j++) {
            StringBuilder str = new StringBuilder();
            int size = sizeList[j];
            int iterNum = 30;

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
//                System.out.println(sql);
//                result = session.executeQueryStatement(sql);
//            }
//            endTime = System.currentTimeMillis();
//            str.append(outputSizeList[j] + "\t");
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\t");

            iterNum = 1;
            startTime = System.currentTimeMillis();
            for (int i = 0; i < iterNum; i++) {
                int minTime = random.nextInt(map.get(curDataset) - size) * 1000;
                int maxTime = minTime + size * 1000;
                String sql = "select mcod(s9, " +
                        "'radius'='" + r + "', " +
                        "'threshold'='" + k + "', " +
                        "'window'='" + w + "', " +
                        "'slide'='" + s + "') " +
                        "from root.udf" + curDataset + ".d0 " +
                        "where time>=" + minTime + " and time<" + maxTime;
                System.out.println(sql);
                result = session.executeQueryStatement(sql);
            }
            endTime = System.currentTimeMillis();
            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
            lw.log(str);
        }
    }
}
