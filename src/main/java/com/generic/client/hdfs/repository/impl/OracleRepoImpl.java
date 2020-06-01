package com.generic.client.hdfs.repository.impl;

import com.generic.client.hdfs.exception.HdfsClientException;
import com.generic.client.hdfs.repository.OracleRepo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Service
public class OracleRepoImpl implements OracleRepo {
    @Value("${DB_DRIVER_CLASS}")
    private static String DB_DRIVER_CLASS;
    @Value("${DB_URL}")
    private static String DB_URL;
    @Value("${DB_USERNAME}")
    private static String DB_USERNAME;
    @Value("${DB_PASSWORD}")
    private static String DB_PASSWORD;
    @Value("${DB_PROC}")
    private static String DB_PROC;


    @Override
    public Connection getOrclConnection() {
        Connection con = null;
        try {
            // load the Driver Class
            Class.forName(DB_DRIVER_CLASS);

            // create the connection now
            con = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            throw new HdfsClientException("Oracle db connectivity exception");
        }
        return con;
    }


    @Override
    public String getColumns(String tablename) {
        Connection con = null;
        CallableStatement stmt = null;
        String result;

        try {
            con = getOrclConnection();
            stmt = con.prepareCall("{call " + DB_PROC + "(?,?)}");
            stmt.setString(1, tablename);


            //register the OUT parameter before calling the stored procedure
            stmt.registerOutParameter(2, java.sql.Types.VARCHAR);

            stmt.executeUpdate();

            //read the OUT parameter now
            result = stmt.getString(6);

        } catch (Exception e) {
            throw new HdfsClientException(e.getMessage());
        } finally {
            try {
                stmt.close();
                con.close();
            } catch (SQLException e) {
                throw new HdfsClientException(e.getMessage());
            }
        }


        return result;
    }
}
