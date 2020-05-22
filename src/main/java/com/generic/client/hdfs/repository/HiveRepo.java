package com.generic.client.hdfs.repository;

import java.sql.Connection;

public interface HiveRepo {

    Connection getHiveCon();
    String execSQL(String sql);


}