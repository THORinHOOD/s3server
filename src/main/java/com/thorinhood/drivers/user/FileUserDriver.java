package com.thorinhood.drivers.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thorinhood.data.S3User;
import com.thorinhood.drivers.FileDriver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Optional;

public class FileUserDriver extends FileDriver implements UserDriver {

    private static final Logger log = LogManager.getLogger(FileUserDriver.class);
    private static final String USER_FILE_NAME = "identity.json";

    private final ObjectMapper objectMapper;

    public FileUserDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void addUser(S3User s3User) throws Exception {
        String userFolderPath = USERS_FOLDER_PATH + File.separatorChar + s3User.getAccessKey();
        createFolder(userFolderPath);
        objectMapper.writeValue(new File(userFolderPath + File.separatorChar + USER_FILE_NAME), s3User);
    }

    @Override
    public Optional<S3User> getS3User(String accessKey) throws Exception {
        String userPath = USERS_FOLDER_PATH + File.separatorChar + accessKey + File.separatorChar + USER_FILE_NAME;
        File file = new File(userPath);
        if (!file.exists() || !file.isFile()) {
            return Optional.empty();
        }
        return Optional.of(objectMapper.readValue(new File(userPath), S3User.class));
    }

    @Override
    public void removeUser(String accessKey) throws Exception {
        String userPath = USERS_FOLDER_PATH + File.separatorChar + accessKey;
        deleteFolder(userPath);
    }

}
