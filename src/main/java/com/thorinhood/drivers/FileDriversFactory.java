package com.thorinhood.drivers;

import com.thorinhood.drivers.acl.AclDriver;
import com.thorinhood.drivers.acl.FileAclDriver;
import com.thorinhood.drivers.entity.EntityDriver;
import com.thorinhood.drivers.entity.FileEntityDriver;
import com.thorinhood.drivers.lock.EntityLocker;
import com.thorinhood.drivers.metadata.FileMetadataDriver;
import com.thorinhood.drivers.metadata.MetadataDriver;
import com.thorinhood.drivers.principal.FilePolicyDriver;
import com.thorinhood.drivers.principal.PolicyDriver;
import com.thorinhood.drivers.user.FileUserDriver;
import com.thorinhood.drivers.user.UserDriver;

import java.io.File;

public class FileDriversFactory extends FileDriver {

    public FileDriversFactory(String baseFolderPath, EntityLocker entityLocker) {
        super(baseFolderPath,
baseFolderPath + File.separatorChar + CONFIG_FOLDER_NAME,
baseFolderPath + File.separatorChar + CONFIG_FOLDER_NAME + File.separatorChar + USERS_FOLDER_NAME,
                entityLocker);
    }

    public void init() throws Exception {
        createFolder(BASE_FOLDER_PATH);
        createFolder(CONFIG_FOLDER_PATH);
        createFolder(USERS_FOLDER_PATH);
    }

    public void clearAll() throws Exception {
        deleteFolder(BASE_FOLDER_PATH);
    }

    public UserDriver createUserDriver() {
        return new FileUserDriver(BASE_FOLDER_PATH, CONFIG_FOLDER_PATH, USERS_FOLDER_PATH, ENTITY_LOCKER);
    }

    public AclDriver createAclDriver() {
        return new FileAclDriver(BASE_FOLDER_PATH, CONFIG_FOLDER_PATH, USERS_FOLDER_PATH, ENTITY_LOCKER);
    }

    public MetadataDriver createMetadataDriver() {
        return new FileMetadataDriver(BASE_FOLDER_PATH, CONFIG_FOLDER_PATH, USERS_FOLDER_PATH, ENTITY_LOCKER);
    }

    public PolicyDriver createPolicyDriver() {
        return new FilePolicyDriver(BASE_FOLDER_PATH, CONFIG_FOLDER_PATH, USERS_FOLDER_PATH, ENTITY_LOCKER);
    }

    public EntityDriver createEntityDriver() {
        return new FileEntityDriver(BASE_FOLDER_PATH, CONFIG_FOLDER_PATH, USERS_FOLDER_PATH, ENTITY_LOCKER);
    }

}
