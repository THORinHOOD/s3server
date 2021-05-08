package com.thorinhood.drivers.entity;

import com.thorinhood.data.GetBucketObjects;
import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.data.S3User;
import com.thorinhood.data.multipart.Part;
import com.thorinhood.data.requests.S3Headers;
import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.data.s3object.HasMetaData;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.drivers.FileDriver;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
    public void createBucket(S3FileBucketPath s3FileBucketPath, S3User s3User) throws S3Exception {
        String absolutePath = s3FileBucketPath.getPathToBucket();
        File bucketFile = new File(absolutePath);
        if (bucketFile.exists()) {
            throw S3Exception.build("Bucket already exists : " + absolutePath)
                    .setStatus(HttpResponseStatus.CONFLICT)
                    .setCode(S3ResponseErrorCodes.BUCKET_ALREADY_OWNED_BY_YOU)
                    .setMessage("Your previous request to create the named bucket succeeded and you already own it.")
                    .setResource(File.separatorChar + s3FileBucketPath.getBucket())
                    .setRequestId("1"); // TODO
        }
        if (!bucketFile.mkdir()) {
            throw S3Exception.INTERNAL_ERROR("Can't create bucket: " + absolutePath)
                    .setMessage("Can't create bucket")
                    .setResource(File.separatorChar + s3FileBucketPath.getBucket())
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public HasMetaData getObject(S3FileObjectPath s3FileObjectPath, String eTag, HttpHeaders httpHeaders)
            throws S3Exception {
        String absolutePath = s3FileObjectPath.getPathToObject();
        File file = new File(absolutePath);
        if (file.isHidden() || !file.exists() || !file.isFile()) {
            throw S3Exception.build("File not found: " + absolutePath)
                    .setStatus(HttpResponseStatus.NOT_FOUND)
                    .setCode(S3ResponseErrorCodes.NO_SUCH_KEY)
                    .setMessage("The resource you requested does not exist")
                    .setResource(File.separatorChar + s3FileObjectPath.getKeyWithBucket())
                    .setRequestId("1"); // TODO
        }
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file.toPath());
            if (httpHeaders != null) {
                checkSelectors(httpHeaders, eTag, file);
            }
            return S3Object.build()
                    .setAbsolutePath(absolutePath)
                    .setS3Path(s3FileObjectPath)
                    .setETag(eTag)
                    .setFile(file)
                    .setRawBytes(bytes)
                    .setLastModified(DateTimeUtil.parseDateTime(file));
        } catch (ParseException | IOException exception) {
            throw S3Exception.INTERNAL_ERROR("Can't create object: " + absolutePath)
                    .setMessage("Internal error : can't create object")
                    .setResource(File.separatorChar + s3FileObjectPath.getKeyWithBucket())
                    .setRequestId("1"); // TODO
        }
    }

    @Override
    public S3Object putObject(S3FileObjectPath s3FileObjectPath, byte[] bytes, Map<String, String> metadata)
            throws S3Exception {
        String absolutePath = s3FileObjectPath.getPathToObject();
        File file = new File(absolutePath);
        Path objectMetadataFolder = new File(s3FileObjectPath.getPathToObjectMetadataFolder()).toPath();
        Path source = createPreparedTmpFile(objectMetadataFolder, file.toPath(), bytes);
        commitFile(source, file.toPath());
        return S3Object.build()
                .setAbsolutePath(absolutePath)
                .setS3Path(s3FileObjectPath)
                .setETag(calculateETag(bytes))
                .setFile(file)
                .setRawBytes(bytes)
                .setLastModified(DateTimeUtil.parseDateTime(file))
                .setMetaData(metadata);
    }

    @Override
    public void deleteObject(S3FileObjectPath s3FileObjectPath) throws S3Exception {
        String pathToObject = s3FileObjectPath.getPathToObject();
        deleteFile(pathToObject);
        deleteFolder(s3FileObjectPath.getPathToObjectMetadataFolder());
        deleteEmptyKeys(new File(pathToObject));
    }

    @Override
    public void deleteBucket(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        String pathToBucket = s3FileBucketPath.getPathToBucket();
        String pathToBucketMetadataFolder = s3FileBucketPath.getPathToBucketMetadataFolder();
        deleteFolder(pathToBucket);
        deleteFolder(pathToBucketMetadataFolder);
    }

    @Override
    public Pair<Pair<List<S3FileObjectPath>, Boolean>, String> getBucketObjects(GetBucketObjects getBucketObjects)
            throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH + File.separatorChar + getBucketObjects.getBucket());
        List<S3FileObjectPath> objects = new ArrayList<>();
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
                            if (getBucketObjects.getStartAfter() == null || getBucketObjects.getStartAfter()
                                    .compareTo(currentPath) <= 0) {
                                if (checkPrefix(getBucketObjects.getPrefix(), currentPath)) {
                                    objects.add(S3FileObjectPath.raw(BASE_FOLDER_PATH, getBucketObjects.getBucket(),
                                            currentPath));
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
    public List<Pair<String, String>> getBuckets(S3User s3User) throws S3Exception {
        Path path = Path.of(BASE_FOLDER_PATH);
        List<Pair<String, String>> buckets;
        try {
            try (Stream<Path> tree = Files.walk(path, 1)) {
                buckets = tree.filter(entity -> isFolderExists(entity) && !isMetadataFolder(entity) &&
                                                !isConfigFolder(entity) && isBucket(entity) && !entity.equals(path))
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
    public void createMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId) throws S3Exception {
        String multipartFolder = s3FileObjectPath.getPathToObjectMultipartFolder();
        createFolder(multipartFolder);
        String currentUploadFolder = s3FileObjectPath.getPathToObjectUploadFolder(uploadId);
        createFolder(currentUploadFolder);
    }

    @Override
    public void abortMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId) throws S3Exception {
        String multipartFolder = s3FileObjectPath.getPathToObjectMultipartFolder();
        if (!isFolderExists(multipartFolder)) {
            return;
        }
        String uploadFolder = s3FileObjectPath.getPathToObjectUploadFolder(uploadId);
        if (!isFolderExists(uploadFolder)) {
            return;
        }
        deleteFolder(uploadFolder);
    }

    @Override
    public String putUploadPart(S3FileObjectPath s3FileObjectPath, String uploadId, int partNumber, byte[] bytes)
            throws S3Exception {
        String currentUploadFolder = s3FileObjectPath.getPathToObjectUploadFolder(uploadId);
        if (!isFolderExists(currentUploadFolder)) {
            throw S3Exception.NO_SUCH_UPLOAD(uploadId);
        }
        String partPathStr = s3FileObjectPath.getPathToObjectUploadPart(uploadId, partNumber);
        Path partPath = new File(partPathStr).toPath();
        Path source = createPreparedTmpFile(new File(currentUploadFolder).toPath(), partPath, bytes);
        commitFile(source, partPath);
        return calculateETag(bytes);
    }

    @Override
    public String completeMultipartUpload(S3FileObjectPath s3FileObjectPath, String uploadId, List<Part> parts)
            throws S3Exception {
        String currentUploadFolder = s3FileObjectPath.getPathToObjectUploadFolder(uploadId);
        if (!isFolderExists(currentUploadFolder)) {
            throw S3Exception.NO_SUCH_UPLOAD(uploadId);
        }
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

            File file = new File(s3FileObjectPath.getPathToObject());
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
