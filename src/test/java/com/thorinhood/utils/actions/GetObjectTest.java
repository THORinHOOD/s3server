package com.thorinhood.utils.actions;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

public class GetObjectTest extends BaseTest {

    public GetObjectTest(String basePath, int port) {
        super(basePath, port);
    }


    @Test
    public void getObjectSimple() throws Exception {
        S3Client s3 = getS3Client(false, ROOT_USER.getAccessKey(), ROOT_USER.getSecretKey());



    }

}
