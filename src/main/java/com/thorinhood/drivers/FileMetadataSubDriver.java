package com.thorinhood.drivers;

import java.io.File;

public abstract class FileMetadataSubDriver extends FileDriver {

    protected static final String METADATA_FOLDER_PREFIX = ".#";

    protected FileMetadataSubDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
    }

    protected String getPathToBucketMetadataFolder(String bucket) {
        return BASE_FOLDER_PATH + File.separatorChar + METADATA_FOLDER_PREFIX + bucket;
    }

    protected String getPathToObjectMetadataFolder(String bucket, String key) {
        String fileName = extractFileName(key);
        String path = extractPathToFile(key, fileName);
        return BASE_FOLDER_PATH + File.separatorChar + bucket + path + METADATA_FOLDER_PREFIX + fileName;
    }

}
