package com.generic.client.hdfs.service.impl;

import com.generic.client.hdfs.exception.CSVParsingException;
import com.generic.client.hdfs.exception.HdfsClientException;
import com.generic.client.hdfs.models.HiveSchema;
import com.generic.client.hdfs.repository.HiveRepo;
import com.generic.client.hdfs.repository.OracleRepo;
import com.generic.client.hdfs.service.HiveService;
import com.generic.client.hdfs.util.HiveUtil;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

@Service
public class HiveServiceImpl implements HiveService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveServiceImpl.class);
    @Autowired
    HiveUtil util;
    @Autowired
    HiveRepo hrepo;
    @Autowired
    OracleRepo orepo;
    @Value("${db_validation}")
    String dbvalidation;

    @Override
    public String createTable(HiveSchema hs) {
        String result = "failed";
        String sql = util.generateSQL(hs);
        LOGGER.info("Generated SQL is :: " + sql);
        result = hrepo.execSQL(sql);
        return result;
    }

    @Override
    public String loadHiveData(MultipartFile multiFile, String tableName) throws CSVParsingException, FileNotFoundException, HdfsClientException {
        String result = "failed";
        LOGGER.info("loadHiveData invoked :: " + tableName);
        if (validate(multiFile, tableName)) {
            String path = util.saveFile(multiFile);
            if (path != null) {
                String sql = util.generateUploadScript(path, tableName);
                result = hrepo.execSQL(sql);
            }
        }


        return result;
    }

    Boolean validate(MultipartFile file, String tablename) throws FileNotFoundException, HdfsClientException {
        boolean result = false;
        LOGGER.info("validate invoked :: " + tablename);
        if (file.getOriginalFilename().endsWith(".csv") && !file.isEmpty())
            result = true;
        else
            throw new CSVParsingException("FILE NOT CSV OR FILE EMPTY");
        if (dbvalidation.equalsIgnoreCase("yes")) {
            if (matchColumns(file, tablename))
                result = true;
            else
                throw new CSVParsingException("Wrong CSV file loaded");
        }
        return result;
    }

    public boolean matchColumns(MultipartFile file, String tablename) throws FileNotFoundException, HdfsClientException {
        boolean result = false;
        BufferedReader br;
        List<String> fromfile = new LinkedList<>();
        List<String> fromDB = new LinkedList<>();
        LOGGER.info("matchColumns invoked :: " + tablename);
        try {
            String fromDBstring = orepo.getColumns(tablename);
            if (fromDBstring.isEmpty())
                throw new HdfsClientException("No columns found from db for tablename " + tablename);
            LOGGER.info("data recevied from oracle table :: ", fromDBstring);
            String line;
            InputStream is = file.getInputStream();
            br = new BufferedReader(new InputStreamReader(is));
            line = br.readLine();
            fromfile = getList(line);
            fromDB = getList(fromDBstring);

            if (CollectionUtils.isEqualCollection(fromfile, fromDB)) {
                result = true;
                LOGGER.info("matchColumns success for :: " + tablename);
            } else
                throw new CSVParsingException("CSV columns are mis-matched");


        } catch (IOException e) {
            System.err.println(e.getMessage());
        }


        return result;
    }

    public LinkedList getList(String line) {
        LinkedList<String> res = new LinkedList<String>();
        String[] column = line.split(",");
        for (String s : column)
            res.add(s);


        return res;
    }

}
