package com.generic.client.hdfs.service.impl;

import com.generic.client.hdfs.models.HiveSchema;
import com.generic.client.hdfs.repository.HiveRepo;
import com.generic.client.hdfs.service.HiveService;
import com.generic.client.hdfs.util.HiveUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class HiveServiceImpl implements HiveService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveServiceImpl.class);
    @Autowired
    HiveUtil util;
    @Autowired
    HiveRepo repo;

    @Override
    public String createTable(HiveSchema hs) {
        String result = "failed";
        String sql = util.generateSQL(hs);
        LOGGER.info("Generated SQL is :: " + sql);
        result = repo.execSQL(sql);
        return result;
    }

    @Override
    public String loadHiveData(MultipartFile multiFile, String tableName) {
        String result = "failed";
        try {
            String path = util.saveFile(multiFile);
            if (path != null) {
                String sql = util.generateUploadScript(path, tableName);
                result = repo.execSQL(sql);
            }

        } catch (Exception e) {
        }

        return result;
    }


}
