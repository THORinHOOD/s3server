package com.thorinhood.drivers.entity;

import com.thorinhood.data.GetBucketObjects;
import com.thorinhood.data.S3BucketPath;
import com.thorinhood.data.S3ObjectPath;
import com.thorinhood.data.S3User;
import com.thorinhood.data.multipart.Part;
import com.thorinhood.data.requests.S3Headers;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.drivers.PreparedOperationFileCommit;
import com.thorinhood.drivers.PreparedOperationFileCommitWithResult;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.selectors.*;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.Pair;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileEntityDriver extends FileDriver implements EntityDriver {

    private static final Logger log = LogManager.getLogger(FileEntityDriver.class);

    private static final String INVALID_PART_MESSAGE = "One or more of the specified parts could not be found. " +
            "The part might not have been uploaded, or the specified entity tag might not " +
            "have matched the part's entity tag.";
    private static final String ENTITY_TOO_SMALL = "Your proposed upload is smaller than the minimum allowed " +
            "object size. Each part must be at least 5 MB in size.";
    private static final long MIN_PART_SIZE = 5242880L;

    private static S3Exception INVALID_PART_EXCEPTION() {
        return S3Exception.build(INVALID_PART_MESSAGE)
                .setStatus(HttpResponseStatus.BAD_REQUEST)
                .setCode(S3ResponseErrorCodes.INVALID_PART)
                .setMessage(INVALID_PART_MESSAGE)
                .setResource("1")
                .setRequestId("1");
    }

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
    public PreparedOperationFileCommitWithResult<S3Object> putObject(S3ObjectPath s3ObjectPath, byte[] bytes,
                                                                     Map<String, String> metadata) throws S3Exception {
        String absolutePath = s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH);
        File file = new File(absolutePath);
        if (!processFolders(file, s3ObjectPath.getBucket())) {
            throw S3Exception.INTERNAL_ERROR("Can't create folders: " + absolutePath)
                    .setMessage("Internal error : can't create folder")
                    .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
                    .setRequestId("1"); // TODO
        }
        Path objectMetadataFolder = new File(getPathToObjectMetadataFolder(s3ObjectPath, true)).toPath();

        S3Object s3Object = S3Object.build()
                .setAbsolutePath(absolutePath)
                .setS3Path(s3ObjectPath)
                .setETag(calculateETag(bytes))
                .setFile(file)
                .setRawBytes(bytes)
                .setLastModified(DateTimeUtil.parseDateTime(file))
                .setMetaData(metadata);
        Path source = createPreparedTmpFile(objectMetadataFolder, file.toPath(), bytes);
        return new PreparedOperationFileCommitWithResult<>(source, file.toPath(), s3Object);
    }

    @Override
    public void deleteObject(S3ObjectPath s3ObjectPath) throws S3Exception {
        String pathToObject = s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH);
        String pathToObjectMetadataFolder = getPathToObjectMetadataFolder(s3ObjectPath, false);
        deleteFolder(pathToObjectMetadataFolder);
        deleteFile(pathToObject);
        deleteEmptyKeys(new File(pathToObject));
    }

    @Override
    public void deleteBucket(S3BucketPath s3BucketPath) throws S3Exception {
        String pathToBucket = s3BucketPath.getFullPathToBucket(BASE_FOLDER_PATH);
        String pathToBucketMetadataFolder = getPathToBucketMetadataFolder(s3BucketPath, false);
        deleteFolder(pathToBucket);
        deleteFolder(pathToBucketMetadataFolder);
    }

    @Override
    public Pair<Pair<List<HasMetaData>, Boolean>, String> getBucketObjects(GetBucketObjects getBucketObjects) throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH + File.separatorChar + getBucketObjects.getBucket());
        List<HasMetaData> objects = new ArrayList<>();
        boolean truncated = false;
        boolean start = false;
        String nextContinuationToken = null;
        try {
            try (Stream<Path> tree = Files.walk(path)) {
                int from = path.toString().length() + 1;
                List<String> allObjects = tree.filter(current -> !isMetadataFolder(current) &&
                        !isMetadataFile(current) && !Files.isDirectory(current))
                        .map(objectPath -> objectPath.toString().substring(from))
                        .sorted(String::compareTo)
                        .collect(Collectors.toList());
                for (int i = 0; (i < allObjects.size()) && !truncated; i++) {
                    String currentPath = allObjects.get(i);
                    String objectMd5 = DigestUtils.md5Hex(currentPath);
                    if (objects.size() == getBucketObjects.getMaxKeys()) {
                        truncated = true;
                        nextContinuationToken = objectMd5;
                    } else {
                        if (getBucketObjects.getContinuationToken() != null) {
                            if (objectMd5.equals(getBucketObjects.getContinuationToken())) {
                                start = true;
                            }
                        } else {
                            start = true;
                        }
                        if (start) {
                            if (getBucketObjects.getStartAfter() == null ||
                                    getBucketObjects.getStartAfter().compareTo(currentPath) <= 0) {
                                if (checkPrefix(getBucketObjects.getPrefix(), currentPath)) {
                                    objects.add(getObject(S3ObjectPath.raw(getBucketObjects.getBucket(), currentPath),
                                            null));
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't list bucket objects")
                    .setMessage("Can't list bucket objects")
                    .setResource("1")
                    .setRequestId("1");
        }
        return Pair.of(Pair.of(objects, truncated), nextContinuationToken);
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
        File metadataFolder = new File(getPathToBucketMetadataFolder(s3BucketPath, false));
        return bucketFile.exists() && bucketFile.isDirectory() && metadataFolder.exists() &&
                metadataFolder.isDirectory();
    }

    @Override
    public boolean isObjectExists(S3ObjectPath s3ObjectPath) throws S3Exception {
        File file = new File(s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH));
        return file.exists() && file.isFile();
    }

    @Override
    public List<Pair<String, String>> getBuckets(S3User s3User) throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH);
        List<Pair<String, String>> buckets;
        try {
            try (Stream<Path> tree = Files.walk(path, 1)) {
                buckets = tree.filter(entity -> !isMetadataFolder(entity) && !isConfigFolder(entity) &&
                        Files.isDirectory(entity) && !entity.equals(path) &&
                        isBucketExists(S3BucketPath.build(entity.getFileName().toString())))
                    .map(entity -> {
                        File bucket = entity.toFile();
                        try {
                            BasicFileAttributes attr = Files.readAttributes(entity, BasicFileAttributes.class);
                            return Pair.of(bucket.getName(),
                                    DateTimeUtil.parseDateTimeISO(attr.creationTime().toMillis()));
                        } catch (IOException e) {
                            throw S3Exception.INTERNAL_ERROR("Can't get bucket attributes :" + entity.getFileName())
                                    .setMessage("Can't get bucket attributes : " + entity.getFileName())
                                    .setResource("1")
                                    .setRequestId("1"); // TODO
                        }
                    }).collect(Collectors.toList());
            }
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't list bucket objects")
                    .setMessage("Can't list bucket objects")
                    .setResource("1")
                    .setRequestId("1");
        }
        return buckets;
    }

    @Override
    public String createMultipartUpload(S3ObjectPath s3ObjectPath) throws S3Exception {
        File tmp = new File(s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH));
        if (!processFolders(tmp, s3ObjectPath.getBucket())) {
            throw S3Exception.INTERNAL_ERROR("Can't create folders: " + tmp.getAbsolutePath())
                    .setMessage("Internal error : can't create folder")
                    .setResource(File.separatorChar + s3ObjectPath.getKeyWithBucket())
                    .setRequestId("1"); // TODO
        }
        String multipartFolder = getPathToObjectMultipartFolder(s3ObjectPath, true);
        String uploadId;
        uploadId = DigestUtils.md5Hex(DateTimeUtil.currentDateTime() + new Random().nextLong() +
                tmp.getAbsolutePath());
        String currentUploadFolder = multipartFolder + File.separatorChar + uploadId;
        createFolder(currentUploadFolder);
        return uploadId;
    }

    @Override
    public void abortMultipartUpload(S3ObjectPath s3ObjectPath, String uploadId) throws S3Exception {
        String multipartFolder = getPathToObjectMultipartFolder(s3ObjectPath, true);
        if (!existsFolder(multipartFolder)) {
            return;
        }
        String uploadFolder = multipartFolder + File.separatorChar + uploadId;
        if (!existsFolder(uploadFolder)) {
            return;
        }
        deleteFolder(uploadFolder); // TODO Clear empty
    }

    @Override
    public String putUploadPart(S3ObjectPath s3ObjectPath, String uploadId, int partNumber, byte[] bytes)
            throws S3Exception {
        String currentUploadFolder = getPathToObjectUploadFolder(s3ObjectPath, uploadId, false);
        String partPathStr = currentUploadFolder + File.separatorChar + partNumber;
        Path partPath = new File(partPathStr).toPath();
        Path source = createPreparedTmpFile(new File(currentUploadFolder).toPath(), partPath, bytes);
        new PreparedOperationFileCommit(source, partPath).lockAndCommit();
        return calculateETag(bytes);
    }

    @Override
    public String completeMultipartUpload(S3ObjectPath s3ObjectPath, String uploadId, List<Part> parts)
            throws S3Exception {
        String currentUploadFolder = getPathToObjectUploadFolder(s3ObjectPath, uploadId, false);
        Path currentUploadFolderPath = Path.of(currentUploadFolder);
        try (Stream<Path> tree = Files.walk(currentUploadFolderPath, 1)) {
            Map<Integer, File> partsFiles = tree
                .filter(current -> !current.toString().equals(currentUploadFolder) && Files.isRegularFile(current) &&
                    current.getFileName().toString().matches("[0-9]+") &&
                    Integer.parseInt(current.getFileName().toString()) >= 1 &&
                    Integer.parseInt(current.getFileName().toString()) <= 10000)
                .collect(Collectors.toMap(p -> Integer.parseInt(p.getFileName().toString()), Path::toFile));

            if (!parts.stream().allMatch(partInfo -> partsFiles.containsKey(partInfo.getPartNumber()))) {
                throw INVALID_PART_EXCEPTION();
            }

            File file = new File(s3ObjectPath.getFullPathToObject(BASE_FOLDER_PATH));
            if (!file.exists() || (file.exists() && file.isDirectory())) {
                if (!file.createNewFile()) {
                    throw new Exception("Can't create file");
                }
            }
            MessageDigest md = MessageDigest.getInstance("MD5");
            try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file, true))) {
                for (Part part : parts) {
                    File partFile = partsFiles.get(part.getPartNumber());
                    long partSizeInBytes = Files.size(partFile.toPath());
                    if (partSizeInBytes < MIN_PART_SIZE) {
                        if (file.exists()) {
                            file.delete();
                        }
                        throw S3Exception.build(ENTITY_TOO_SMALL)
                                .setStatus(HttpResponseStatus.BAD_REQUEST)
                                .setCode(S3ResponseErrorCodes.ENTITY_TOO_SMALL)
                                .setMessage(ENTITY_TOO_SMALL)
                                .setResource("1")
                                .setRequestId("1");
                    }
                    try (InputStream input = new BufferedInputStream(new FileInputStream(partFile))) {
                        byte[] buffer = input.readAllBytes();
                        String calculatedEtag = DigestUtils.md5Hex(buffer);
                        if (!part.getETag().equals(calculatedEtag)) {
                            throw INVALID_PART_EXCEPTION();
                        }
                        outputStream.write(buffer);
                        md.update(buffer);
                    }
                }
            }
            deleteFolder(currentUploadFolder);
            return DatatypeConverter.printHexBinary(md.digest()).toLowerCase();
        } catch (Exception e) {
            throw S3Exception.INTERNAL_ERROR("Can't process multipart upload : " + currentUploadFolder)
                    .setMessage("Can't process multipart upload : " + currentUploadFolder)
                    .setResource("1")
                    .setRequestId("1");
        }
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

    private String getPathToObjectUploadFolder(S3ObjectPath s3ObjectPath, String uploadId, boolean safely) {
        String multipartFolder = getPathToObjectMultipartFolder(s3ObjectPath, safely);
        String currentUploadFolder = multipartFolder + File.separatorChar + uploadId;
        if (!existsFolder(currentUploadFolder)) {
            throw S3Exception.build("No such upload : " + uploadId)
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_UPLOAD)
                    .setMessage("The specified multipart upload does not exist. The upload ID might be invalid, " +
                            "or the multipart upload might have been aborted or completed.")
                    .setResource("1")
                    .setRequestId("1"); // TODO
        }
        return currentUploadFolder;
    }

    private String getPathToObjectMultipartFolder(S3ObjectPath s3ObjectPath, boolean safely) {
        String path = getPathToObjectMetadataFolder(s3ObjectPath, safely) + File.separatorChar + MULTIPART_FOLDER_NAME;
        if (safely) {
            createFolder(path);
        }
        return path;
    }

}
