package com.thorinhood.utils.actions;

import com.thorinhood.Server;
import com.thorinhood.data.S3User;
import com.thorinhood.drivers.FileDriversFactory;
import com.thorinhood.drivers.acl.AclDriver;
import com.thorinhood.drivers.entity.EntityDriver;
import com.thorinhood.drivers.lock.EntityLocker;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.drivers.main.S3FileDriverImpl;
import com.thorinhood.drivers.metadata.MetadataDriver;
import com.thorinhood.drivers.principal.PolicyDriver;
import com.thorinhood.drivers.user.UserDriver;
import com.thorinhood.utils.utils.SdkUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BaseTest {

    protected FileDriversFactory FILE_DRIVERS_FACTORY;
    protected UserDriver USER_DRIVER;
    protected AclDriver ACL_DRIVER;
    protected MetadataDriver METADATA_DRIVER;
    protected PolicyDriver POLICY_DRIVER;
    protected EntityDriver ENTITY_DRIVER;
    protected S3Driver S3_DRIVER;

    protected Server SERVER;
    protected S3User ROOT_USER;
    protected S3User NOT_AUTH_ROOT_USER;
    protected S3User ROOT_USER_2;
    protected List<S3User> ALL_TEST_USERS;

    private final int port;
    protected final String BASE_PATH;

    private Thread serverThread;

    private List<S3Client> s3ClientsCash;
    private List<S3AsyncClient> s3AsyncClientsCash;

    public BaseTest(String baseFolderName, int port) {
        String home = System.getenv("HOME");
        String basePath = home + File.separatorChar + baseFolderName;
        this.BASE_PATH = basePath;
        this.port = port;
        createUsers();
        EntityLocker entityLocker = new EntityLocker();
        FILE_DRIVERS_FACTORY = new FileDriversFactory(basePath, entityLocker);
        USER_DRIVER = FILE_DRIVERS_FACTORY.createUserDriver();
        ACL_DRIVER = FILE_DRIVERS_FACTORY.createAclDriver();
        METADATA_DRIVER = FILE_DRIVERS_FACTORY.createMetadataDriver();
        POLICY_DRIVER = FILE_DRIVERS_FACTORY.createPolicyDriver();
        ENTITY_DRIVER = FILE_DRIVERS_FACTORY.createEntityDriver();
        S3_DRIVER = new S3FileDriverImpl(METADATA_DRIVER, ACL_DRIVER, POLICY_DRIVER, ENTITY_DRIVER, entityLocker,
                basePath);
        SERVER = new Server(port, S3_DRIVER, USER_DRIVER);
    }


    @BeforeEach
    void beforeEach() throws Exception {
        reloadFs();
    }

    @AfterEach
    void afterEach() {
        if (s3ClientsCash != null && !s3ClientsCash.isEmpty()) {
            s3ClientsCash.forEach(SdkAutoCloseable::close);
            s3ClientsCash.clear();
        }
        if (s3AsyncClientsCash != null && !s3AsyncClientsCash.isEmpty()) {
            s3AsyncClientsCash.forEach(SdkAutoCloseable::close);
            s3AsyncClientsCash.clear();
        }
    }

    @BeforeAll
    void init() throws Exception {
        FILE_DRIVERS_FACTORY.clearAll();
        FILE_DRIVERS_FACTORY.init();
        initUsers();
        serverThread = new Thread(() -> {
            try {
                SERVER.run();
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        });
        serverThread.start();
    }

    @AfterAll
    void clean() throws Exception {
        SERVER.stop();
        FILE_DRIVERS_FACTORY.clearAll();
        serverThread.interrupt();
    }

    void reloadFs() throws Exception {
        FILE_DRIVERS_FACTORY.clearAll();
        FILE_DRIVERS_FACTORY.init();
        initUsers();
    }

    protected S3Client getS3Client(boolean chunked, String accessKey, String secretKey) {
        S3Client s3Client = SdkUtil.build(port, Region.US_WEST_2, chunked, accessKey, secretKey);
        if (s3ClientsCash == null) {
            s3ClientsCash = new ArrayList<>();
        }
        s3ClientsCash.add(s3Client);
        return s3Client;
    }

    protected S3AsyncClient getS3AsyncClient(boolean chunked, String accessKey, String secretKey) {
        S3AsyncClient s3AsyncClient = SdkUtil.buildAsync(port, Region.US_WEST_2, chunked, accessKey, secretKey);
        if (s3AsyncClientsCash == null) {
            s3AsyncClientsCash = new ArrayList<>();
        }
        s3AsyncClientsCash.add(s3AsyncClient);
        return s3AsyncClient;
    }

    protected void initUsers() {
        ALL_TEST_USERS.forEach(s3User -> USER_DRIVER.addUser(s3User));
    }

    protected void createBucketRaw(S3Client s3Client, String bucket) {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucket)
                .build();
        s3Client.createBucket(request);
    }

    protected void putObjectRaw(S3Client s3Client, String bucket, String key, String content,
                                Map<String, String> metadata) {
        PutObjectRequest.Builder request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key);
        if (metadata != null) {
            request.metadata(metadata);
        }
        s3Client.putObject(request.build(), RequestBody.fromString(content));
    }

    protected void checkObjectNotExists(String bucket, String keyWithoutName, String fileName) throws IOException {
        File file = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                buildKey(keyWithoutName, fileName));
        Assertions.assertTrue(!file.exists() || !file.isFile());
        File acl = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                (keyWithoutName == null || keyWithoutName.isEmpty() ? ".#" + fileName + File.separatorChar +
                        fileName + ".acl" :
                        keyWithoutName + File.separatorChar + ".#" + fileName + File.separatorChar + fileName +
                                ".acl"));
        Assertions.assertTrue(!acl.exists() || !acl.isFile());
        File metadataFile = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                (keyWithoutName == null || keyWithoutName.isEmpty() ? ".#" + fileName + File.separatorChar +
                        fileName + ".meta" :
                        keyWithoutName + File.separatorChar + ".#" + fileName + File.separatorChar + fileName +
                                ".meta"));
        Assertions.assertTrue(!metadataFile.exists() || !metadataFile.isFile());
    }


    protected void checkObject(String bucket, String keyWithoutName, String fileName, String content,
                               Map<String, String> metadata) throws IOException {
        checkObject(bucket, keyWithoutName, fileName, content, metadata, true);
    }

    protected void checkObject(String bucket, String keyWithoutName, String fileName, String content,
                               Map<String, String> metadata, boolean checkContent) throws IOException {
        File file = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                buildKey(keyWithoutName, fileName));
        Assertions.assertTrue(file.exists() && file.isFile());
        File acl = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                (keyWithoutName == null || keyWithoutName.isEmpty() ? ".#" + fileName + File.separatorChar +
                        fileName + ".acl" :
                        keyWithoutName + File.separatorChar + ".#" + fileName + File.separatorChar + fileName + ".acl"));
        Assertions.assertTrue(acl.exists() && acl.isFile());
        if (checkContent) {
            checkContent(file, content);
        }
        if (metadata != null) {
            checkObjectMetadata(bucket, keyWithoutName, fileName, metadata);
        }
    }

    private void checkObjectMetadata(String bucket, String keyWithoutName, String fileName,
                                     Map<String, String> metadata) throws IOException {
        File metadataFile = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                (keyWithoutName == null || keyWithoutName.isEmpty() ? ".#" + fileName + File.separatorChar +
                        fileName + ".meta" :
                        keyWithoutName + File.separatorChar + ".#" + fileName + File.separatorChar + fileName + ".meta"));
        Assertions.assertTrue(metadataFile.exists() && metadataFile.isFile());
        String metadataActualString = Files.readString(metadataFile.toPath());
        Map<String, String> actualMetadata = Arrays.stream(metadataActualString.split("\n"))
                .collect(Collectors.toMap(
                        line -> line.substring(0, line.indexOf("=")),
                        line -> line.substring(line.indexOf("=") + 1)
                ));
        assertMaps(metadata, actualMetadata);
    }

    protected void checkContent(File file, List<String> contents) throws IOException {
        String actual = Files.readString(file.toPath());
        Assertions.assertTrue(contents.stream().anyMatch(content -> content.equals(actual)));
    }

    private void checkContent(File file, String expected) throws IOException {
        String actual = Files.readString(file.toPath());
        Assertions.assertEquals(expected, actual);
    }

    protected String buildKey(String keyWithoutFileName, String fileName) {
        return (keyWithoutFileName == null || keyWithoutFileName.isEmpty() ? fileName : keyWithoutFileName +
                File.separatorChar + fileName);
    }

    protected String calcETag(String content) {
        return DigestUtils.md5Hex(content.getBytes());
    }

    protected String createContent(int bytes) {
        char[] chars = new char[bytes];
        Arrays.fill(chars, 'a');
        return new String(chars);
    }

    protected List<CompletableFuture<ResponseBytes<GetObjectResponse>>> getObjectAsync(S3AsyncClient s3, String bucket,
                                                           String key, String ifMatch,
                                                           String ifNoneMatch, int requestsCount) throws Exception {
        GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key);
        if (ifMatch != null) {
            requestBuilder.ifMatch(ifMatch);
        }
        if (ifNoneMatch != null) {
            requestBuilder.ifNoneMatch(ifNoneMatch);
        }
        GetObjectRequest request = requestBuilder.build();
        List<CompletableFuture<ResponseBytes<GetObjectResponse>>> futureList = new ArrayList<>();
        for (int i = 0; i < requestsCount; i++) {
            futureList.add(s3.getObject(request, AsyncResponseTransformer.toBytes()));
        }
        return futureList;
    }

    protected void checkGetObjectAsync(List<CompletableFuture<ResponseBytes<GetObjectResponse>>> futureList,
           List<String> content, List<Map<String, String>> metadata) throws ExecutionException, InterruptedException {
        for (int i = 0; i < futureList.size(); i++) {
            ResponseBytes<GetObjectResponse> resp;
            try {
                resp = futureList.get(i).get();
            } catch (Exception exception) {
                Assertions.fail("FAIL : " + i);
                throw exception;
            }
            boolean ok = false;
            for (int j = 0; (j < content.size()) && !ok; j++) {
                String expectedContent = content.get(j);
                Map<String, String> expectedMetadata = metadata.get(j);
                boolean contentLength = resp.response().contentLength()
                        .equals((long) expectedContent.getBytes().length);
                boolean eTag = calcETag(expectedContent).equals(resp.response().eTag());
                boolean meta = expectedMetadata != null ? equalsMaps(expectedMetadata, resp.response().metadata()) :
                        (resp.response().metadata() == null || resp.response().metadata().isEmpty());
                boolean contentEq = expectedContent.equals(new String(resp.asByteArray()));
                if (contentEq && eTag && meta && contentLength) {
                    ok = true;
                }
            }
            if (!ok) {
                Assertions.fail("Request got wrong content : length = " + resp.response().contentLength() + "(" +
                        i + "/" + futureList.size() + ")\n" + resp);
            }
        }
    }

    protected void checkGetObject(String expectedContent, Map<String, String> expectedMetadata,
                                  ResponseBytes<GetObjectResponse> response) {
        Assertions.assertEquals(expectedContent.getBytes().length, response.response().contentLength());
        Assertions.assertEquals(calcETag(expectedContent), response.response().eTag());
        if (expectedMetadata != null) {
            assertMaps(expectedMetadata, response.response().metadata());
        } else {
            Assertions.assertTrue(response.response().metadata() == null ||
                    response.response().metadata().isEmpty());
        }
        Assertions.assertEquals(expectedContent, new String(response.asByteArray()));
    }

    protected List<CompletableFuture<PutObjectResponse>> putObjectAsync(S3AsyncClient s3, String bucket,
                                    String keyWithoutName, String fileName,
                                    List<String> contents, List<Map<String, String>> metadata) throws IOException {
        List<PutObjectRequest> requests = new ArrayList<>();
        List<AsyncRequestBody> bodies = new ArrayList<>();
        for (int i = 0; i < contents.size(); i++) {
            PutObjectRequest.Builder request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(buildKey(keyWithoutName, fileName));
            if (metadata != null) {
                if (metadata.get(i)!= null) {
                    request.metadata(metadata.get(i));
                }
            }
            requests.add(request.build());
            bodies.add(AsyncRequestBody.fromString(contents.get(i)));
        }

        List<CompletableFuture<PutObjectResponse>> futureList = new ArrayList<>();
        for (int i = 0; i < requests.size(); i++) {
            futureList.add(s3.putObject(requests.get(i), bodies.get(i)));
        }
        return futureList;

    }

    protected void checkPutObjectAsync(String bucket, String keyWithoutName, String fileName,
                                       List<CompletableFuture<PutObjectResponse>> futureList, List<String> contents,
                                       List<Map<String, String>> metadata) throws IOException {
        for (int i = 0; i < futureList.size(); i++) {
            try {
                PutObjectResponse response = futureList.get(i).get();
                Assertions.assertEquals(response.eTag(), calcETag(contents.get(i % 2)));
            } catch (InterruptedException | ExecutionException e) {
                Assertions.fail(e);
            }
        }
        checkObject(bucket, keyWithoutName, fileName, null, null, false);
        checkContent(new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar + keyWithoutName +
                File.separatorChar + fileName), contents);
    }

    protected <KEY, VALUE> boolean equalsMaps(Map<KEY, VALUE> expected, Map<KEY, VALUE> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (Map.Entry<KEY, VALUE> expectedEntry : expected.entrySet()) {
            if (!actual.containsKey(expectedEntry.getKey())) {
                return false;
            }
            if (!expectedEntry.getValue().equals(actual.get(expectedEntry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    protected <KEY, VALUE> void assertMaps(Map<KEY, VALUE> expected, Map<KEY, VALUE> actual) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (Map.Entry<KEY, VALUE> expectedEntry : expected.entrySet()) {
            Assertions.assertTrue(actual.containsKey(expectedEntry.getKey()));
            Assertions.assertEquals(expectedEntry.getValue(), actual.get(expectedEntry.getKey()));
        }
    }

    private void createUsers() {
        ROOT_USER = rootUser();
        ROOT_USER_2 = rootUser3();
        NOT_AUTH_ROOT_USER = notAuthRootUser();
        ALL_TEST_USERS = new ArrayList<>();
        ALL_TEST_USERS.add(ROOT_USER);
        ALL_TEST_USERS.add(ROOT_USER_2);
    }

    private S3User notAuthRootUser() {
        return new S3User(
                "accessKey2",
                "secretKey2",
                null,
                null,
                "userId2",
                "arn:aws:s3:::userId2:root",
                "canonicalUserId2",
                "testRootUser2"
        );
    }

    private S3User rootUser3() {
        return new S3User(
                "accessKey3",
                "secretKey3",
                null,
                null,
                "userId3",
                "arn:aws:s3:::userId3:root",
                "canonicalUserId3",
                "testRootUser3"
        );
    }

    private S3User rootUser() {
        return new S3User(
                "accessKey",
                "secretKey",
                null,
                null,
                "userId",
                "arn:aws:s3:::userId:root",
                "canonicalUserId",
                "testRootUser"
        );
    }

}
