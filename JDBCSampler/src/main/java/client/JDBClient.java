package client;

import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * @author wmarkito
 *         2015
 */
public class JDBClient {

    public static void main(String[] args) throws InterruptedException {
        final String URL = "jdbc:gemfirexd://localhost:1527/";
        final String SQL = "SELECT * FROM TOPS_GETS_TOOL_MP_CCA_BASE_H";

        int counter=0;
        try {
            // 1527 is the default port that a GemFire XD server uses to listen for thin client connections
            Connection conn = DriverManager.getConnection(URL);

            final PreparedStatement preparedStatement = conn.prepareStatement(SQL);

            while (true) {
                long start = System.nanoTime();
                ResultSet results = preparedStatement.executeQuery();
                long queryTime = System.nanoTime() - start;

                int rowcount = 0;

                start = System.nanoTime();
                while (results.next())
                    Thread.currentThread().sleep(10000);
                    rowcount++;
                long iterTime = System.nanoTime() - start;

                if (rowcount > 0)
                    System.out.println("Statement executed. Counted all rows. Query time:" + TimeUnit.NANOSECONDS.toMillis(queryTime) + "ms Iteration time:" + TimeUnit.NANOSECONDS.toMillis(iterTime) + "ms " + rowcount + " rows");
                else {
                    System.out.println("Statement executed. Counted all rows. Query time:" + TimeUnit.NANOSECONDS.toMillis(queryTime) + "ms Iteration time:" + TimeUnit.NANOSECONDS.toMillis(iterTime) + "ms " + rowcount + " rows");
                    counter++;

                    if (counter == 5)
                        break;
                }

//                Thread.currentThread().sleep(1000);
            }

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
    }
}
