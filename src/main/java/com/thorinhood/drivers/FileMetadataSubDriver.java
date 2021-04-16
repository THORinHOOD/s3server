package com.thorinhood.drivers;

import com.thorinhood.exceptions.S3Exception;

import java.io.File;

public abstract class FileMetadataSubDriver extends FileDriver {

    protected static final String METADATA_FOLDER_PREFIX = ".#";

    protected FileMetadataSubDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
    }

    protected String getPathToBucketMetadataFolder(String bucket) throws S3Exception {
        String path = BASE_FOLDER_PATH + File.separatorChar + METADATA_FOLDER_PREFIX + bucket;
        createFolder(path);
        return path;
    }

    protected String getPathToObjectMetadataFolder(String bucket, String key) throws S3Exception {
        String fileName = extractFileName(key);
        String path = extractPathToFile(key, fileName);
        String result = BASE_FOLDER_PATH + File.separatorChar + bucket + path + METADATA_FOLDER_PREFIX + fileName;
        createFolder(result);
        return result;
    }

}
