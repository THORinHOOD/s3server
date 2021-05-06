package com.thorinhood.utils.actions;

import com.thorinhood.Server;
import com.thorinhood.data.S3User;
import com.thorinhood.drivers.FileDriversFactory;
import com.thorinhood.drivers.acl.AclDriver;
import com.thorinhood.drivers.entity.EntityDriver;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.drivers.main.S3DriverImpl;
import com.thorinhood.drivers.metadata.MetadataDriver;
import com.thorinhood.drivers.principal.PolicyDriver;
import com.thorinhood.drivers.user.UserDriver;
import com.thorinhood.utils.utils.SdkUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    public BaseTest(String baseFolderName, int port) {
        String home = System.getenv("HOME");
        String basePath = home + File.separatorChar + baseFolderName;
        this.BASE_PATH = basePath;
        this.port = port;
        createUsers();
        FILE_DRIVERS_FACTORY = new FileDriversFactory(basePath);
        USER_DRIVER = FILE_DRIVERS_FACTORY.createUserDriver();
        ACL_DRIVER = FILE_DRIVERS_FACTORY.createAclDriver();
        METADATA_DRIVER = FILE_DRIVERS_FACTORY.createMetadataDriver();
        POLICY_DRIVER = FILE_DRIVERS_FACTORY.createPolicyDriver();
        ENTITY_DRIVER = FILE_DRIVERS_FACTORY.createEntityDriver();
        S3_DRIVER = new S3DriverImpl(METADATA_DRIVER, ACL_DRIVER, POLICY_DRIVER, ENTITY_DRIVER);
        SERVER = new Server(port, S3_DRIVER, USER_DRIVER);
    }


    @BeforeEach
    void beforeEach() throws Exception {
        reloadFs();
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
        return SdkUtil.build(port, Region.US_WEST_2, chunked, accessKey, secretKey);
    }

    protected S3AsyncClient getS3AsyncClient(boolean chunked, String accessKey, String secretKey) {
        return SdkUtil.buildAsync(port, Region.US_WEST_2, chunked, accessKey, secretKey);
    }

    protected void initUsers() {
        ALL_TEST_USERS.forEach(s3User -> USER_DRIVER.addUser(s3User));
    }

    protected void createBucketRaw(String bucket, S3Client s3Client) {
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucket)
                .build();
        try {
            s3Client.createBucket(request);
        } catch (Exception exception) {
        }
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
        File file = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                buildKey(keyWithoutName, fileName));
        Assertions.assertTrue(file.exists() && file.isFile());
        File acl = new File(BASE_PATH + File.separatorChar + bucket + File.separatorChar +
                (keyWithoutName == null || keyWithoutName.isEmpty() ? ".#" + fileName + File.separatorChar +
                        fileName + ".acl" :
                        keyWithoutName + File.separatorChar + ".#" + fileName + File.separatorChar + fileName + ".acl"));
        Assertions.assertTrue(acl.exists() && acl.isFile());
        checkContent(file, content);
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
