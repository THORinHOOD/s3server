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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    protected final S3Client S3_CLIENT;

    private final int port;
    protected final String BASE_PATH;

    private Thread serverThread;

    public BaseTest(String basePath, int port) {
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
        S3_CLIENT = SdkUtil.build(port, Region.US_WEST_2, false, "accessKey",
                "secretKey");
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

    protected S3Client getS3Client(boolean chunked, String accesskey, String secretKey) {
        return SdkUtil.build(port, Region.US_WEST_2, chunked, accesskey, secretKey);
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

    protected String calcETag(String content) {
        return DigestUtils.md5Hex(content.getBytes());
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
