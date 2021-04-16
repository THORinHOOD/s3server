package com.thorinhood.drivers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

public class FileDriver {

    private static final Logger log = LogManager.getLogger(FileDriver.class);

    protected static final String CONFIG_FOLDER_NAME = ".config";
    protected static final String USERS_FOLDER_NAME = "users";
    protected static final String METADATA_FOLDER_PREFIX = "#";

    protected final String BASE_FOLDER_PATH;
    protected final String CONFIG_FOLDER_PATH;
    protected final String USERS_FOLDER_PATH;

    protected FileDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        this.BASE_FOLDER_PATH = baseFolderPath;
        this.CONFIG_FOLDER_PATH = configFolderPath;
        this.USERS_FOLDER_PATH = usersFolderPath;
    }

    protected boolean existsFolder(String path) {
        File folder = new File(path);
        return folder.isDirectory() && folder.exists();
    }

    protected void createFolder(String path) throws Exception {
        File folder = new File(path);
        if (folder.exists() && !folder.isDirectory()) {
            exception(path + " is not a folder");
        }
        if (!folder.exists() && !folder.mkdir()) {
            exception("Can't create folder " + folder);
        }
    }

    protected void deleteFolder(String path) throws Exception {
        File folder = new File(path);
        if (folder.exists() && folder.isDirectory()) {
            if (!folder.delete()) {
                exception("Can't delete folder " + path);
            }
        }
    }

    protected void exception(String msg) throws Exception {
        log.error(msg);
        throw new Exception(msg);
    }

    protected String extractFileName(String key) {
        return key.substring(key.lastIndexOf("/") + 1);
    }

    protected String extractPathToFile(String key, String fileName) {
        return key.substring(0, key.indexOf(fileName));
    }

}
