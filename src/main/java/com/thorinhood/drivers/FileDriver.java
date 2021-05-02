package com.thorinhood.drivers;

import com.thorinhood.data.S3BucketPath;
import com.thorinhood.data.S3ObjectPath;
import com.thorinhood.exceptions.S3Exception;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class FileDriver {

    private static final Logger log = LogManager.getLogger(FileDriver.class);

    protected static final String METADATA_FOLDER_PREFIX = ".#";
    protected static final String CONFIG_FOLDER_NAME = ".##config";
    protected static final String USERS_FOLDER_NAME = "users";

    protected final String BASE_FOLDER_PATH;
    protected final String CONFIG_FOLDER_PATH;
    protected final String USERS_FOLDER_PATH;

    protected FileDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        this.BASE_FOLDER_PATH = baseFolderPath;
        this.CONFIG_FOLDER_PATH = configFolderPath;
        this.USERS_FOLDER_PATH = usersFolderPath;
    }

    protected String getPathToBucketMetadataFolder(S3BucketPath s3BucketPath, boolean safely) throws S3Exception {
        if (s3BucketPath.getBucket() == null) {
            throw S3Exception.INTERNAL_ERROR("Can't build path to bucket metadata folder : " + s3BucketPath)
                    .setMessage("Can't build path to bucket metadata folder : " + s3BucketPath)
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
        String path = BASE_FOLDER_PATH + File.separatorChar + METADATA_FOLDER_PREFIX + s3BucketPath.getBucket();
        if (safely) {
            createFolder(path);
        }
        return path;
    }

    protected String getPathToObjectMetadataFolder(S3ObjectPath s3ObjectPath, boolean safely) throws S3Exception {
        if (s3ObjectPath.getName() == null) {
            throw S3Exception.INTERNAL_ERROR("Object name is null : " + s3ObjectPath)
                    .setMessage("Object name is null : " + s3ObjectPath)
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
        String result = s3ObjectPath.getFullPathToObjectFolder(BASE_FOLDER_PATH) + METADATA_FOLDER_PREFIX +
                s3ObjectPath.getName();
        if (safely) {
            createFolder(result);
        }
        return result;
    }

    protected boolean existsFolder(String path) {
        File folder = new File(path);
        return folder.isDirectory() && folder.exists();
    }

    protected void createFolder(String path) throws S3Exception {
        File folder = new File(path);
        if (folder.exists() && !folder.isDirectory()) {
            exception(path + " is not a folder");
        }
        if (!folder.exists() && !folder.mkdir()) {
            exception("Can't create folder " + folder);
        }
    }

    protected void deleteFolder(String path) throws S3Exception {
        File folder = new File(path);
        if (folder.exists() && folder.isDirectory()) {
            try {
                Files.walkFileTree(folder.toPath(), new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }
                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        if (exc == null) {
                            Files.delete(dir);
                            return FileVisitResult.CONTINUE;
                        } else {
                            throw exc;
                        }
                    }
                });
            } catch (IOException exception) {
                exception("Can't delete folder " + path);
            }
        }
    }

    protected void deleteFile(String path) throws S3Exception {
        File file = new File(path);
        if (file.exists() && file.isFile()) {
            if (!file.delete()) {
                exception("Can't delete file " + path);
            }
        }
    }

    protected void exception(String msg) throws S3Exception {
        log.error(msg);
        throw S3Exception.INTERNAL_ERROR(msg)
                .setMessage(msg)
                .setResource("1")
                .setRequestId("1"); // TODO
    }

}
