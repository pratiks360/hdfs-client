package com.generic.client.hdfs.service;

import com.generic.client.hdfs.exception.HdfsClientException;
import com.generic.client.hdfs.models.HiveSchema;
import org.springframework.web.multipart.MultipartFile;

import java.io.FileNotFoundException;

public interface HiveService {

    String createTable(HiveSchema hs);
    String loadHiveData(MultipartFile multiFile,String tableName) throws FileNotFoundException, HdfsClientException;

}
