package com.generic.client.hdfs.service;

import com.generic.client.hdfs.models.HiveSchema;
import org.springframework.web.multipart.MultipartFile;

public interface HiveService {

    String createTable(HiveSchema hs);
    String loadHiveData(MultipartFile multiFile,String tableName);

}
