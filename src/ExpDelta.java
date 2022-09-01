import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpDelta {
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

        Map<String, Integer> map = new HashMap<>();
        map.put("jinfeng", 1000000);
        map.put("tianyuan", 1000002);
        map.put("PAMAP2", 299999);
        map.put("wanhua", 1000000);

        int k=50,  w = 20,s = 10;
        int iterNum = 100;

        double r = 0;
        int size = 0;
        switch (curDataset){
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


        LogWriter lw = new LogWriter("./result/ExpDelta/" + curDataset + "_res.dat");
        lw.open();

        lw.log("Size\tLSMOD(our)" + "\n");
        Random random = new Random();

        StringBuilder str = new StringBuilder();


        /**
         Tsfile
         */
        startTime = System.currentTimeMillis();
        for (int i = 0; i < iterNum; i++) {
            int minTime = random.nextInt(map.get(curDataset) - size) * 1000;
            int maxTime = minTime + size * 1000;
            String sql = "select dodds(s9, " +
                    "'r'='" + r + "', " +
                    "'k'='" + k + "', " +
                    "'w'='" + w + "', " +
                    "'s'='" + s + "') " +
                    "from root." + curDataset + ".d0 " +
                    "where time>=" + minTime + " and time<" + maxTime;
            result = session.executeQueryStatement(sql);
        }
        endTime = System.currentTimeMillis();

        str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
        lw.log(str);
    }
}
