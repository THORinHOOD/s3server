package com.thorinhood.drivers.metadata;

import com.thorinhood.data.S3ObjectPath;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.drivers.PreparedOperationFileCommit;
import com.thorinhood.exceptions.S3Exception;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class FileMetadataDriver extends FileDriver implements MetadataDriver {

    public FileMetadataDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
    }

    @Override
    public PreparedOperationFileCommit putObjectMetadata(S3ObjectPath s3ObjectPath,
                                                         Map<String, String> metadata) throws S3Exception {
        File file = new File(getObjectMetaFile(s3ObjectPath, true));
        String pathToMetadataFolder = getPathToObjectMetadataFolder(s3ObjectPath, true);
        StringBuilder content = new StringBuilder();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            content.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        Path source = createPreparedTmpFile(new File(pathToMetadataFolder).toPath(), file.toPath(),
                content.toString().getBytes());
        return new PreparedOperationFileCommit(source, file.toPath());
    }

    @Override
    public Map<String, String> getObjectMetadata(S3ObjectPath s3ObjectPath) throws S3Exception {
        File file = new File(getObjectMetaFile(s3ObjectPath, false));
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

    private String getBucketMetaFile(S3ObjectPath s3ObjectPath, boolean safely) {
        String pathToMetaFolder = getPathToBucketMetadataFolder(s3ObjectPath, safely);
        return pathToMetaFolder + File.separatorChar + s3ObjectPath.getBucket() + ".meta";
    }

    private String getObjectMetaFile(S3ObjectPath s3ObjectPath, boolean safely) {
        String pathToMetaFolder = getPathToObjectMetadataFolder(s3ObjectPath, safely);
        return pathToMetaFolder + File.separatorChar + s3ObjectPath.getName() + ".meta";
    }

}
