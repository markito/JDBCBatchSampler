package io.pivotal.gemfire.example.jmeter;

import io.pivotal.gemfire.example.generator.BatchDataGenerator;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.protocol.jdbc.config.DataSourceElement;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by wmarkito on 11/21/14.
 */
public class JDBCBatchSampler extends AbstractJavaSamplerClient {

    private static final Logger LOGGER = LoggingManager.getLoggerForClass();
    //private static final ThreadLocal<BatchDataGenerator> batchDataGenerator = new ThreadLocal<BatchDataGenerator>();

    @Override
    public org.apache.jmeter.samplers.SampleResult runTest(JavaSamplerContext context) {

        final String RESULT_OK = "OK";
        final String RESULT_FAIL = "FAIL";

        // from JavaRequest properties
        int batchSize = context.getIntParameter("Batch size");
        // from JDBC Configuration element
        String dbPool = context.getParameter("Connection Pool Name");
        // SQL to be used for prepareStatement
        String sqlString = context.getParameter("SQL with Params");
        // fields TYPE,SIZE format
        String fieldPair = context.getParameter("Fields Type and Size");

        Connection conn = null;
        PreparedStatement pstm = null;

        SampleResult result = new SampleResult();

        // generate data before sampling
        String[] randomData = BatchDataGenerator.generateRandomData(fieldPair, batchSize);
        //String[] randomData = batchDataGenerator.get().generateRandomData(fieldPair, batchSize);

        try {

            result.sampleStart();
            conn = DataSourceElement.getConnection(dbPool);

            conn.setAutoCommit(false);
            pstm = conn.prepareStatement(sqlString);

            int paramCount = pstm.getParameterMetaData().getParameterCount();

            for (String rowData: randomData) {
                String[] fields = rowData.split(",");

                for (int i = 1; i <= paramCount; i++) {
                    pstm.setString(i, fields[i-1]);
                }

                pstm.addBatch();
            }

            // submit
            pstm.executeBatch();
            conn.commit();
            pstm.clearBatch();

            result.setSampleCount(batchSize);
            result.setSuccessful(true);
            result.setResponseMessage(RESULT_OK);
            result.setResponseCodeOK();

        } catch (SQLException e) {

            LOGGER.error(e.getMessage());

            result.setResponseMessage(e.getMessage());
            result.setSuccessful(false);
            result.setResponseCode(RESULT_FAIL);

            rollBack(conn);

        } finally {

            result.sampleEnd();
            closeResource(conn, pstm);
        }

        return result;
    }

    private void closeResource(Connection conn, PreparedStatement pstm) {
        try {
            if (pstm != null) pstm.close();
            if (conn != null) conn.close();

        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    private void rollBack(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException sqlex) {
            sqlex.printStackTrace();
        }
    }

    /**
     * Default values for parameters
     * @return
     */
    public Arguments getDefaultParameters()
    {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("Batch size", "50");
        defaultParameters.addArgument("Connection Pool Name", "pool");
        defaultParameters.addArgument("SQL with Params", "INSERT INTO table VALUES (?,?)");
        defaultParameters.addArgument("Fields Type and Size", "VARCHAR,30;VARCHAR,30;");

        return defaultParameters;
    }

}