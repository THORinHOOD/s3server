package com.thorinhood.drivers;

import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.data.S3FileStatic;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.Random;

public class FileDriver {

    private static final Logger log = LogManager.getLogger(FileDriver.class);

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

    public boolean isConfigFolder(Path path) {
        return Files.isDirectory(path) && path.getFileName().toString().equals(CONFIG_FOLDER_NAME);
    }

    public boolean isMetadataFolder(Path path) {
        return Files.isDirectory(path) && path.getFileName().toString().startsWith(S3FileStatic.METADATA_FOLDER_PREFIX);
    }

    protected boolean isMetadataFile(Path path) {
        return Files.isRegularFile(path) && isMetadataFolder(path.getParent());
    }

    public boolean isBucket(Path path) {
        return Files.isDirectory(path) && path.getParent().toString().equals(BASE_FOLDER_PATH);
    }

    protected void commitFile(Path source, Path target) throws S3Exception {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception)
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
    }

    protected Path createPreparedTmpFile(Path tmpFolder, Path file, byte[] bytes) {
        File tmpFile = new File(tmpFolder.toAbsolutePath().toString() + File.separatorChar +
            file.getFileName().toString() + "." + new Random().nextLong());
        while (tmpFile.exists()) {
            tmpFile = new File(tmpFolder.toAbsolutePath().toString() + File.separatorChar +
                    file.getFileName().toString() + "." + new Random().nextLong());
        }
        try {
            if (tmpFile.createNewFile()) {
                try (FileOutputStream outputStream = new FileOutputStream(tmpFile)) {
                    outputStream.write(bytes);
                }
                return tmpFile.toPath();
            } else {
                throw S3Exception.INTERNAL_ERROR("Can't create file : " + tmpFile.getAbsolutePath())
                        .setMessage("Internal error : can't create file")
                        .setResource("1")
                        .setRequestId("1"); // TODO
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception.getMessage())
                    .setMessage(exception.getMessage())
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
    }

    public void createFolder(String path) throws S3Exception {
        File folder = new File(path);
        if (!folder.exists() && !folder.mkdirs()) {
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

    protected void deleteEmptyKeys(File file) throws S3Exception {
        try {
            Path parent = file.toPath().getParent();
            while (!isBucket(parent) && !parent.toString().equals(BASE_FOLDER_PATH) &&
                    Objects.requireNonNull(parent.toFile().list()).length == 0) {
                Files.delete(parent);
                parent = parent.getParent();
            }
        } catch (Exception e) {
            throw S3Exception.INTERNAL_ERROR(e.getMessage())
                    .setMessage(e.getMessage())
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
    }

    protected void deleteFile(String path) throws S3Exception {
        File file = new File(path);
        if (isFileExists(file)) {
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

    public void checkObject(S3FileObjectPath s3FileObjectPath) throws S3Exception {
        Path pathToObject = new File(s3FileObjectPath.getPathToObject()).toPath();
        if (!isFileExists(pathToObject)) {
            throw S3Exception.build("File not found : " + pathToObject.toAbsolutePath())
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_KEY)
                    .setMessage("The resource you requested does not exist")
                    .setResource("1")
                    .setRequestId("1");
        }
        Path pathToObjectMetadataFolder = new File(s3FileObjectPath.getPathToObjectMetadataFolder()).toPath();
        Path pathToObjectMetaFile = new File(s3FileObjectPath.getPathToObjectMetaFile()).toPath();
        Path pathToObjectAclFile = new File(s3FileObjectPath.getPathToObjectAclFile()).toPath();
        if (!isFolderExists(pathToObjectMetadataFolder) || !isMetadataFolder(pathToObjectMetadataFolder) ||
            !isFileExists(pathToObjectMetaFile) || !isMetadataFile(pathToObjectMetaFile) ||
            !isFileExists(pathToObjectAclFile) || !isMetadataFile(pathToObjectAclFile)) {
            throw S3Exception.INTERNAL_ERROR("Object is currupt")
                    .setMessage("Object is currupt")
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    public void checkBucket(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        Path pathToBucket = new File(s3FileBucketPath.getPathToBucket()).toPath();
        if (!isFolderExists(pathToBucket) || !isBucket(pathToBucket)) {
            throw S3Exception.build("Bucket does not exist")
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_BUCKET)
                    .setMessage("The specified bucket does not exist")
                    .setResource("1")
                    .setRequestId("1");
        }
        Path pathToMetadataBucket = new File(s3FileBucketPath.getPathToBucketMetadataFolder()).toPath();
        Path pathToBucketAcl = new File(s3FileBucketPath.getPathToBucketAclFile()).toPath();
        if (!isFolderExists(pathToMetadataBucket) || !isMetadataFolder(pathToMetadataBucket) ||
            !isFileExists(pathToBucketAcl) || !isMetadataFile(pathToBucketAcl)) {
            throw S3Exception.INTERNAL_ERROR("Bucket is currupt")
                    .setMessage("Bucket is currupt")
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    public boolean isFolderExists(String path) {
        File folder = new File(path);
        return folder.exists() && folder.isDirectory();
    }

    public boolean isFolderExists(File folder) {
        return folder.exists() && folder.isDirectory();
    }

    public boolean isFolderExists(Path folder) {
        return isFolderExists(folder.toFile());
    }

    public boolean isFileExists(String path) {
        File file = new File(path);
        return file.exists() && file.isFile();
    }

    public boolean isFileExists(File file) {
        return file.exists() && file.isFile();
    }

    public boolean isFileExists(Path path) {
        return isFileExists(path.toFile());
    }

    public S3FileObjectPath buildPathToObject(String bucketKey) {
        return S3FileObjectPath.relative(BASE_FOLDER_PATH, bucketKey.substring(1));
    }
}
