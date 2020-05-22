package com.generic.client.hdfs.repository.impl;

import com.generic.client.hdfs.repository.HiveRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Service
public class HiveRepoImpl implements HiveRepo {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveRepoImpl.class);
    @Value("${hiveDriver}")
    String driverName;
    @Value("${hiveConnUrl}")
    String connUrl;
    @Value("${hiveUser}")
    String user;
    @Value("${hivePass}")
    String pass;


    @Override
    public Connection getHiveCon() {
        Connection con = null;
        try {
            LOGGER.info("Creating Hive Connection");
            Class.forName(driverName);
            con = DriverManager.getConnection(connUrl, user, pass);
        } catch (ClassNotFoundException e) {
            LOGGER.error("Error in Creation of Hive Connection in", e);
        } catch (SQLException throwables) {
            LOGGER.error("Error in Creation of Hive Connection in", throwables);
        }
        return con;
    }

    @Override
    public String execSQL(String sql) {
        String result = "failed";
        LOGGER.info("Creating Hive Table");
        try {
            Connection con = getHiveCon();
            if (con != null) {
                Statement stmt = con.createStatement();
                stmt.executeQuery(sql);
                result = "success";
            }
        } catch (SQLException throwables) {
            LOGGER.error("Error in hive table Creation", throwables);
        }
        return result;
    }


}
