package com.thorinhood.drivers.entity;

import com.thorinhood.data.*;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.selectors.*;
import com.thorinhood.utils.DateTimeUtil;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class FileEntityDriver extends FileDriver implements EntityDriver {

    private static final Logger log = LogManager.getLogger(FileEntityDriver.class);

    private final Map<String, Selector<String>> strSelectors;
    private final Map<String, Selector<Date>> dateSelectors;

    public FileEntityDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
        strSelectors = Map.of(
                S3Headers.IF_MATCH, new IfMatch(),
                S3Headers.IF_NONE_MATCH, new IfNoneMatch()
        );
        dateSelectors = Map.of(
                S3Headers.IF_MODIFIED_SINCE, new IfModifiedSince(),
                S3Headers.IF_UNMODIFIED_SINCE, new IfUnmodifiedSince()
        );
    }

    @Override
    public void createBucket(S3BucketPath s3BucketPath, S3User s3User) throws S3Exception {
        String absolutePath = s3BucketPath.getFullPathToBucket(BASE_FOLDER_PATH);
        File bucketFile = new File(absolutePath);
        if (bucketFile.exists()) {
            throw S3Exception.build("Bucket already exists : " + absolutePath)
                    .setStatus(HttpResponseStatus.CONFLICT)
                    .setCode(S3ResponseErrorCodes.BUCKET_ALREADY_OWNED_BY_YOU)
                    .setMessage("Your previous request to create the named bucket succeeded and you already own it.")
                    .setResource(File.separatorChar + s3BucketPath.getBucket())
                    .setRequestId("1"); // TODO
        }
        if (!bucketFile.mkdir()) {
            throw S3Exception.INTERNAL_ERROR("Can't create bucket: " + absolutePath)
                    .setMessage("Can't create bucket")
                    .setResource(File.separatorChar + s3BucketPath.getBucket())
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public HasMetaData getObject(S3ObjectPath s3ObjectPath, HttpHeaders httpHeaders) throws S3Exception {
        String absolutePath = s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH);
        File file = new File(absolutePath);
        if (file.isHidden() || !file.exists() || !file.isFile()) {
            throw S3Exception.build("File not found: " + absolutePath)
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_KEY)
                    .setMessage("The resource you requested does not exist")
                    .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
                    .setRequestId("1"); // TODO
        }

        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file.toPath());
            String eTag = calculateETag(bytes);
            if (httpHeaders != null) {
                checkSelectors(httpHeaders, eTag, file);
            }
            return S3Object.build()
                    .setAbsolutePath(absolutePath)
                    .setS3Path(s3ObjectPath)
                    .setETag(eTag)
                    .setFile(file)
                    .setRawBytes(bytes)
                    .setLastModified(DateTimeUtil.parseDateTime(file));
        } catch (IOException | ParseException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage("Internal error : can't create object")
                    .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public S3Object putObject(S3ObjectPath s3ObjectPath, byte[] bytes, Map<String, String> metadata) throws S3Exception {
        String absolutePath = s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH);
        File file = new File(absolutePath);
        if (!processFolders(file, s3ObjectPath.getBucket())) {
            throw S3Exception.INTERNAL_ERROR("Can't create folders: " + absolutePath)
                    .setMessage("Internal error : can't create folder")
                    .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
                    .setRequestId("1"); // TODO
        }
        try {
            log.info("Starting creating file : " + file.getAbsolutePath() + " # " + file.getPath());
            if (file.createNewFile() || file.exists()) {
                FileOutputStream outputStream = new FileOutputStream(file);
                outputStream.write(bytes);
                outputStream.close();
                return S3Object.build()
                        .setAbsolutePath(absolutePath)
                        .setS3Path(s3ObjectPath)
                        .setETag(calculateETag(bytes))
                        .setFile(file)
                        .setRawBytes(bytes)
                        .setLastModified(DateTimeUtil.parseDateTime(file))
                        .setMetaData(metadata);
            } else {
                throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                        .setMessage("Internal error : can't create object")
                        .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
                        .setRequestId("1"); // TODO
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage(exception.getMessage())
                    .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public void deleteObject(S3ObjectPath s3ObjectPath) throws S3Exception {
        String pathToObject = s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH);
        String pathToObjectMetadataFolder = getPathToObjectMetadataFolder(s3ObjectPath, false);
        deleteFolder(pathToObjectMetadataFolder);
        deleteFile(pathToObject);
    }

    @Override
    public void deleteBucket(S3BucketPath s3BucketPath) throws S3Exception {
        String pathToBucket = s3BucketPath.getFullPathToBucket(BASE_FOLDER_PATH);
        String pathToBucketMetadataFolder = getPathToBucketMetadataFolder(s3BucketPath, false);
        deleteFolder(pathToBucket);
        deleteFolder(pathToBucketMetadataFolder);
    }

    @Override
    public List<HasMetaData> getBucketObjects(GetBucketObjectsRequest request) throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH + File.separatorChar + request.getBucket());
        List<HasMetaData> objects = new ArrayList<>();
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (dir.getFileName().toString().startsWith(".#")) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    int from = dir.toString().indexOf(request.getBucket()) + request.getBucket().length() + 1;
                    if (from >= dir.toString().length()) {
                        return FileVisitResult.CONTINUE;
                    }
                    String key = dir.toString().substring(from);
                    return checkPrefix(request.getPrefix(), key) ? FileVisitResult.CONTINUE :
                            FileVisitResult.SKIP_SUBTREE;
                }
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    int indexOfBucket = file.toString().indexOf(request.getBucket());
                    int from = indexOfBucket + request.getBucket().length() + 1;
                    String path = file.toString().substring(from);
                    if (!checkPrefix(request.getPrefix(), path)) {
                        return FileVisitResult.CONTINUE;
                    }
                    String key = file.toString().substring(indexOfBucket);
                    objects.add(getObject(S3ObjectPath.relative(key), null));
                    if (objects.size() >= request.getMaxKeys()) {
                        return FileVisitResult.TERMINATE;
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't list bucket objects")
                    .setMessage("Can't list bucket objects")
                    .setResource("1")
                    .setRequestId("1");
        }
        return objects;
    }

    private boolean checkPrefix(String prefix, String path) {
        if (prefix == null) {
            return true;
        }
        return path.startsWith(prefix);
    }

    @Override
    public boolean isBucketExists(S3BucketPath s3BucketPath) throws S3Exception {
        File bucketFile = new File(s3BucketPath.getFullPathToBucket(BASE_FOLDER_PATH));
        return bucketFile.exists() && bucketFile.isDirectory();
    }

    @Override
    public boolean isObjectExists(S3ObjectPath s3ObjectPath) throws S3Exception {
        File file = new File(s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH));
        return file.exists() && file.isFile();
    }

    private boolean processFolders(File file, String bucket) {
        File folder = file.getParentFile();
        if (folder.exists() && folder.isDirectory()) {
            return true;
        } else if (folder.exists() && !folder.isDirectory()) {
            return false;
        } else if (!folder.getName().equals(bucket)) {
            return processFolders(folder, bucket) && folder.mkdir();
        }
        return true;
    }

    private String calculateETag(byte[] bytes) {
        return DigestUtils.md5Hex(bytes);
    }

    private void checkSelectors(HttpHeaders headers, String eTag, File file) throws ParseException {
        //TODO
        if (headers.contains(S3Headers.IF_MATCH)) {
            strSelectors.get(S3Headers.IF_MATCH).check(eTag, headers.get(S3Headers.IF_MATCH));
        }
        if (headers.contains(S3Headers.IF_MODIFIED_SINCE)) {
            dateSelectors.get(S3Headers.IF_MODIFIED_SINCE).check(new Date(file.lastModified()),
                    DateTimeUtil.parseStrTime(headers.get(S3Headers.IF_MODIFIED_SINCE))); //TODO
        }
        if (headers.contains(S3Headers.IF_NONE_MATCH)) {
            strSelectors.get(S3Headers.IF_NONE_MATCH).check(eTag, headers.get(S3Headers.IF_NONE_MATCH));
        }
        if (headers.contains(S3Headers.IF_UNMODIFIED_SINCE)) {
            dateSelectors.get(S3Headers.IF_UNMODIFIED_SINCE).check(new Date(file.lastModified()),
                    DateTimeUtil.parseStrTime(headers.get(S3Headers.IF_UNMODIFIED_SINCE))); // TODO
        }
    }
}
