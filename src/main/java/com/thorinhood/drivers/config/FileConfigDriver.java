package com.thorinhood.drivers.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thorinhood.data.S3User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Optional;

public class FileConfigDriver implements ConfigDriver {

    private static final Logger log = LogManager.getLogger(FileConfigDriver.class);
    private static final String CONFIG_FOLDER_NAME = ".config";
    private static final String USERS_FOLDER_NAME = "users";
    private static final String USER_FILE_NAME = "identity.json";

    private final String baseFolderPath;
    private String configFolderPath;
    private String usersFolderPath;
    private final ObjectMapper objectMapper;

    public FileConfigDriver(String baseFolderPath) {
        this.baseFolderPath = baseFolderPath;
        objectMapper = new ObjectMapper();
    }

    @Override
    public void init() throws Exception {
        configFolderPath = baseFolderPath + File.separatorChar + CONFIG_FOLDER_NAME;
        usersFolderPath = configFolderPath + File.separatorChar + USERS_FOLDER_NAME;
        createFolder(baseFolderPath);
        createFolder(usersFolderPath);
        createFolder(usersFolderPath);
    }

    @Override
    public void addUser(S3User s3User) throws Exception {
        String userFolderPath = usersFolderPath + File.separatorChar + s3User.getAccessKey();
        createFolder(userFolderPath);
        objectMapper.writeValue(new File(userFolderPath + File.separatorChar + USER_FILE_NAME), s3User);
    }

    @Override
    public Optional<S3User> getS3User(String accessKey) throws Exception {
        String userPath = usersFolderPath + File.separatorChar + accessKey + File.separatorChar + USER_FILE_NAME;
        File file = new File(userPath);
        if (!file.exists() || !file.isFile()) {
            return Optional.empty();
        }
        return Optional.of(objectMapper.readValue(new File(userPath), S3User.class));
    }

    @Override
    public void removeUser(String accessKey) throws Exception {
        String userPath = usersFolderPath + File.separatorChar + accessKey;
        File file = new File(userPath);
        if (file.exists() && file.isDirectory()) {
            if (!file.delete()) {
                exception("Can't delete folder " + userPath);
            }
        }
    }

    private boolean existsFolder(String path) {
        File folder = new File(path);
        return folder.isDirectory() && folder.exists();
    }

    private void createFolder(String path) throws Exception {
        File folder = new File(path);
        if (folder.exists() && !folder.isDirectory()) {
            exception(path + " is not a folder");
        }
        if (!folder.exists() && !folder.mkdir()) {
            exception("Can't create folder " + folder);
        }
    }

    private void exception(String msg) throws Exception {
        log.error(msg);
        throw new Exception(msg);
    }

    public String getBaseFolderPath() {
        return baseFolderPath;
    }

}
