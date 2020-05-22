package com.generic.client.hdfs.controller;

import com.generic.client.hdfs.models.HiveSchema;
import com.generic.client.hdfs.service.HiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/api/hdfs/hive")
public class HiveController {
    @Autowired
    HiveService service;

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveController.class);

    @PostMapping("/create")
    public String createTable(@RequestBody HiveSchema hs) {
        LOGGER.info("Hive Create Table Invoked for table name :: " + hs.getTable_name());

        return service.createTable(hs);
    }

    @PostMapping("/upload-csv-file")
    public String uploadCSVFile(@RequestParam("file") MultipartFile multiFile, @RequestParam("TableName") String tableName) {
        String result = null;
        // validate file
        if (multiFile.isEmpty()) {

        } else {
            result = service.loadHiveData(multiFile, tableName);
        }
        return result;
    }

}
