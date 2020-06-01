package com.generic.client.hdfs.repository;

import java.sql.Connection;

public interface OracleRepo {

    Connection getOrclConnection();

    String getColumns(String tablename);
}

