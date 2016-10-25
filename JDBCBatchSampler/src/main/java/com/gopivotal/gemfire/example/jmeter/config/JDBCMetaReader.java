package com.gopivotal.gemfire.example.jmeter.config;

import java.sql.*;
import java.util.logging.Logger;

/**
 * <code>JDBCMetaReader</code> will read table columns and can produce insert statements with params (?) for JDBC
 * or a list of column types with sizes, that be used to setup <code>JDBCBatchSampler</code>
 * @author markito
 */
public class JDBCMetaReader {

    // options
    enum OPT {INSERT, TYPEPAIR};

    private static final Logger LOGGER = Logger.getLogger(JDBCMetaReader.class.getCanonicalName());

    // database properties
    final static String URL = System.getProperty("URL", "jdbc:gemfirexd://localhost:1527/");
    final static String USER = System.getProperty("USER", "APP");
    final static String PASS = System.getProperty("PASSWORD", "APP");
    final static String SCHEMA = System.getProperty("SCHEMA", "ISBAN");

    // run type
    final static String TYPE = System.getProperty("TYPE", "INSERT").toUpperCase();

    public static void main(String[] args) {

        Connection conn = null;
        ResultSet resultSet = null;

        try {


            conn = DriverManager.getConnection(URL, USER, PASS);
            DatabaseMetaData meta = conn.getMetaData();
            resultSet = meta.getTables(null, SCHEMA, "%", null);

            if (TYPE.equals(OPT.INSERT.toString())) {
                generateInsertWithParams(meta, resultSet);
            } else if (TYPE.equals(OPT.TYPEPAIR.toString())) {
                generateFieldTypePair(meta, resultSet);
            } else {
                System.out.println("Error: Parameters not recognized or well formatted");
                printHelp();
            }

            // generateInsertStmt(meta, resultSet);
            //generateFieldTypePair(meta, resultSet);
            //generateInsertWithParams(meta, resultSet);
//
//            List<String> columns = new ArrayList<String>();
//            while (rs.next()) {
//                // 1: none
//                // 2: schema
//                // 3: table name
//                // 4: column name
//                // 5: length
//                // 6: data type (CHAR, VARCHAR, TIMESTAMP, ...)
//                System.out.println(rs.getString(4) + "- TYPE:" + rs.getString(6) + "- SIZE:" + rs.getString("COLUMN_SIZE"));
//               //columns.add(rs.getString(4));
//            }


        } catch (SQLException ex) {

            LOGGER.severe("SQLException: " + ex.getMessage());
            LOGGER.severe("SQLState: " + ex.getSQLState());

        } catch (Exception ex) {

            LOGGER.severe("Error: " + ex.getMessage());

        } finally {
            try {
                if (resultSet != null) resultSet.close();
                if (conn != null) conn.close();

            } catch (Exception ex) {
                LOGGER.severe(ex.getMessage());
            }
        }
    }

    private static void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("System properties available: \r\n");
        sb.append("-DUSER=[APP] -DPASS=[APP] -DURL=[jdbc:gemfirexd://localhost:1527/] -DSCHEMA=[APP] -DTYPE=[INSERT, TYPEPAIR] \r\n");

        System.out.println(sb.toString());
    }


    /**
     * Print insert statements like INSERT INTO TABLE VALUES (?,?,?)
     * @param meta DatabaseMetaData
     * @param rsTable
     * @throws SQLException
     */
    private static void generateInsertWithParams(DatabaseMetaData meta, ResultSet rsTable) throws SQLException {
        while (rsTable.next()) {
            String currentTable = rsTable.getString(3);
            LOGGER.info(String.format("Current Table: %s", currentTable));

            ResultSet rsColumns = meta.getColumns("%", SCHEMA, currentTable, "%");
            StringBuilder insertStmtVars = new StringBuilder();

            insertStmtVars.append("INSERT INTO ").append(currentTable).append(" VALUES (");
            while (rsColumns.next()) {
                insertStmtVars.append("?").append(",");
            }
            insertStmtVars.deleteCharAt(insertStmtVars.lastIndexOf(","));

            insertStmtVars.append(")");
            LOGGER.info(insertStmtVars.toString());
        }
    }

    /**
     * Print string pairs like TYPE,SIZE; for every column on a table
     * Example: VARCHAR,30;CHAR,15;TIMESTAMP,26;CHAR,10;
     * @param meta DatabaseMetaData
     * @param rsTable
     * @throws SQLException
     */
    private static void generateFieldTypePair(DatabaseMetaData meta, ResultSet rsTable) throws SQLException {
        while (rsTable.next()) {
            String currentTable = rsTable.getString(3);
            LOGGER.info(String.format("Current Table: %s", currentTable));

            ResultSet rsColumns = meta.getColumns("%", SCHEMA, currentTable, "%");
            StringBuilder csvTypes = new StringBuilder();

            while (rsColumns.next()) {
                String type = rsColumns.getString(6);
                csvTypes.append(type).append(",").append(rsColumns.getString("COLUMN_SIZE")).append(";");
            }

            LOGGER.info(csvTypes.toString());
        }
    }


}
