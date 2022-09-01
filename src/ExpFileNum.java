import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.filter.operator.In;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpFileNum {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;
        long startTime, endTime;


        Map<String, Integer> map = new HashMap<>();
        map.put("jinfeng", 1000000);
        map.put("tianyuan", 1000002);
        map.put("PAMAP2", 299999);
        map.put("wanhua", 1000000);
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
        // TODO: modify here
        String[] datasetList = {"tianyuan", "wanhua", "jinfeng", "PAMAP2"};
        String curDataset = datasetList[3];
        double r = 0.4;
        int k=50, w=20, s=10;
        //int size = 999600
        int size = 299400;

        LogWriter lw = new LogWriter("./result/ExpFileNum/" + curDataset + "_res.dat");
        lw.open();
        lw.log("Size\tTsfile" + "\n");

        Random random = new Random();

        startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            int minTime = random.nextInt(map.get(curDataset) - size) * 1000;
            int maxTime = minTime + size * 1000;
            String sql = "select dodds(s9, " +
                    "'r'='" + r + "', " +
                    "'k'='" + k + "', " +
                    "'w'='" + w + "', " +
                    "'s'='" + s + "') " +
                    "from root." + curDataset + ".d0 " +
                    "where time>=" + minTime + " and time<" + maxTime;
            System.out.println(sql);
            result = session.executeQueryStatement(sql);
        }
        endTime = System.currentTimeMillis();
        StringBuilder str = new StringBuilder();
        str.append( "\t");
        str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / 100) + "\n");
        lw.log(str);
    }
}
