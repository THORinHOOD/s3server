package com.thorinhood.drivers.metadata;

import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.exceptions.S3Exception;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class FileMetadataDriver extends FileDriver implements MetadataDriver {

    public static final String ETAG = " (additional) etag";

    public FileMetadataDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
    }

    @Override
    public void putObjectMetadata(S3FileObjectPath s3FileObjectPath, Map<String, String> metadata, String eTag)
            throws S3Exception {
        File file = new File(s3FileObjectPath.getPathToObjectMetaFile());
        StringBuilder content = new StringBuilder();
        if (metadata != null) {
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                content.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
            }
        }
        content.append(ETAG).append("=").append(eTag).append("\n");
        Path source = createPreparedTmpFile(new File(s3FileObjectPath.getPathToObjectMetadataFolder()).toPath(),
                file.toPath(), content.toString().getBytes());
        commitFile(source, file.toPath());
    }

    @Override
    public Map<String, String> getObjectMetadata(S3FileObjectPath s3FileObjectPath) throws S3Exception {
        File file = new File(s3FileObjectPath.getPathToObjectMetaFile());
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
        } catch (IOException e) {
            throw S3Exception.INTERNAL_ERROR(e);
        }
        return metadata;
    }

}
