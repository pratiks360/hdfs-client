package com.generic.client.hdfs.util;

import com.generic.client.hdfs.models.HiveColumns;
import com.generic.client.hdfs.models.HiveSchema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

@Component
public class HiveUtil {
    @Value("${uploadPath}")
    String path;

    public String generateSQL(HiveSchema hs) {
        List<HiveColumns> lhc = hs.getColumns();
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE TABLE IF NOT EXISTS ");
        sb.append(hs.getTable_name());
        sb.append(" (");
        Iterator<HiveColumns> iterator = lhc.iterator();
        while (iterator.hasNext()) {
            HiveColumns hc = iterator.next();
            sb.append(hc.getColumn_name());
            sb.append(" ");
            sb.append(hc.getDataType());
            if (iterator.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(",String batch_id) LOCATION ");
        sb.append(hs.getLocation());

        return sb.toString();

    }

    public String saveFile(MultipartFile multiFile) {
        File file = null;
        try {
            String name = multiFile.getOriginalFilename();
            System.out.println(name);

            byte[] bytes = multiFile.getBytes();
            File directory = new File(path);
            directory.mkdirs();
            file = new File(directory.getAbsolutePath() + System.getProperty("file.separator") + name);
            BufferedOutputStream stream = new BufferedOutputStream(
                    new FileOutputStream(file));
            stream.write(bytes);
            stream.close();
            path = file.getAbsolutePath();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return path;

    }

    public String generateUploadScript(String filePath, String tableName) {
        StringBuffer sb = new StringBuffer();
        sb.append("LOAD DATA LOCAL INPATH '");
        sb.append(filePath);
        sb.append("' OVERWRITE INTO TABLE ");
        sb.append(tableName);
        sb.append(";");
        return sb.toString();

    }
}
