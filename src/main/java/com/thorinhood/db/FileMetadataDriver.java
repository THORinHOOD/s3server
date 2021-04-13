package com.thorinhood.db;

import com.thorinhood.exceptions.S3Exception;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FileMetadataDriver implements MetadataDriver {

    @Override
    public boolean init() throws Exception {
        return true;
    }

    @Override
    public void setObjectMetadata(String key, Map<String, String> metadata) throws S3Exception {
        File file = new File(getMetaFile(key));
        try {
            if (!file.exists() && !file.createNewFile()) {
                throw S3Exception.INTERNAL_ERROR("Can't create meta file")
                        .setMessage("Can't create meta file")
                        .setResource("1")
                        .setRequestId("1");
            }
            try (FileOutputStream writer = new FileOutputStream(file.getAbsolutePath())) {
                for (Map.Entry<String, String> entry : metadata.entrySet()) {
                    writer.write((entry.getKey() + "=" + entry.getValue() + "\n").getBytes());
                }
                writer.flush();
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception.getMessage())
                    .setMessage(exception.getMessage())
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    @Override
    public Map<String, String> getObjectMetadata(String key) throws S3Exception {
        File file = new File(getMetaFile(key));
        try {
            if (!file.exists()) {
                return Map.of();
            }
            Map<String, String> metadata = new HashMap<>();
            StringBuilder currentLine = new StringBuilder();
            String keyMeta = null;
            try (FileInputStream reader = new FileInputStream(file.getAbsolutePath())) {
                int currentInt;
                while((currentInt = reader.read()) != -1){
                    char currentChar = (char) currentInt;
                    if (currentChar == '=') {
                        keyMeta = currentLine.toString();
                        currentLine.setLength(0);
                    } else if (currentChar == '\n') {
                        metadata.put(keyMeta, currentLine.toString());
                        currentLine.setLength(0);
                    } else {
                        currentLine.append(currentChar);
                    }
                }
            }
            return metadata;
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception.getMessage())
                    .setMessage(exception.getMessage())
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    private String getMetaFile(String key) {
        return key.substring(0, key.lastIndexOf(".")) + ".meta";
    }

}
