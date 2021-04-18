package com.thorinhood.drivers.metadata;

import com.thorinhood.drivers.FileDriver;
import com.thorinhood.exceptions.S3Exception;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FileMetadataDriver extends FileDriver implements MetadataDriver {

    public FileMetadataDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
    }

    @Override
    public void putObjectMetadata(String bucket, String key, Map<String, String> metadata) throws S3Exception {
        File file = new File(getObjectMetaFile(bucket, key, true));
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
    public Map<String, String> getObjectMetadata(String bucket, String key) throws S3Exception {
        File file = new File(getObjectMetaFile(bucket, key, false));
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

    private String getBucketMetaFile(String bucket, boolean safely) {
        String pathToMetaFolder = getPathToBucketMetadataFolder(bucket, safely);
        return pathToMetaFolder + File.separatorChar + bucket + ".meta";
    }

    private String getObjectMetaFile(String bucket, String key, boolean safely) {
        String pathToMetaFolder = getPathToObjectMetadataFolder(bucket, key, safely);
        return pathToMetaFolder + File.separatorChar + key + ".meta";
    }

}
