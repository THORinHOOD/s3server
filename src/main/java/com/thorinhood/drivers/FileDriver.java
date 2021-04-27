package com.thorinhood.drivers;

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

    protected String getPathToBucketMetadataFolder(String bucket, boolean safely) throws S3Exception {
        String path = BASE_FOLDER_PATH + File.separatorChar + METADATA_FOLDER_PREFIX + bucket;
        if (safely) {
            createFolder(path);
        }
        return path;
    }

    protected String getPathToObjectMetadataFolder(String bucket, String key, boolean safely) throws S3Exception {
        String fileName = extractFileName(key);
        String path = extractPathToFile(key, fileName);
        String result = BASE_FOLDER_PATH + File.separatorChar + bucket + path + METADATA_FOLDER_PREFIX + fileName;
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

    protected String extractFileName(String key) {
        return key.substring(key.lastIndexOf("/") + 1);
    }

    protected String extractPathToFile(String key, String fileName) {
        return key.substring(0, key.indexOf(fileName));
    }

    protected String fullPath(String bucket, String key) {
        return BASE_FOLDER_PATH + File.separatorChar + bucket + key;
    }

    protected String fullPathToBucket(String bucket) {
        return BASE_FOLDER_PATH + File.separatorChar + bucket;
    }

}
