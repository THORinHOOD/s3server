package com.thorinhood.drivers.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thorinhood.data.S3User;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.exceptions.S3Exception;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
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
    public void addUser(S3User s3User) throws S3Exception {
        String userFolderPath = USERS_FOLDER_PATH + File.separatorChar + s3User.getAccessKey();
        createFolder(userFolderPath);
        try {
            objectMapper.writeValue(new File(userFolderPath + File.separatorChar + USER_FILE_NAME), s3User);
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create user")
                    .setMessage("Can't create user")
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public void addUser(String pathToIdentity) throws Exception {
        File userFile = new File(pathToIdentity);
        if (!userFile.exists() || !userFile.isFile()) {
            throw new Exception("Can't find user json : " + pathToIdentity);
        }
        S3User s3User;
        try {
            s3User = objectMapper.readValue(userFile, S3User.class);
        } catch (IOException exception) {
            throw new Exception("Can't parse user json file : " + pathToIdentity);
        }
        addUser(s3User);
    }

    @Override
    public Optional<S3User> getS3User(String accessKey) throws S3Exception {
        String userPath = USERS_FOLDER_PATH + File.separatorChar + accessKey + File.separatorChar + USER_FILE_NAME;
        File file = new File(userPath);
        if (!file.exists() || !file.isFile()) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(new File(userPath), S3User.class));
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't get user")
                    .setMessage("Can't get user")
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public void removeUser(String accessKey) throws S3Exception {
        String userPath = USERS_FOLDER_PATH + File.separatorChar + accessKey;
        deleteFolder(userPath);
    }

}
