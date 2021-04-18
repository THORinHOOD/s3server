package com.thorinhood.utils.actions;

import com.thorinhood.utils.utils.SdkUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class PutObjectTest {

    private S3Client s3;

    @Test
    void putObjectSimple() {
        s3 = SdkUtil.build(9999, Region.US_WEST_2, false, "accessKey", "secretKey");


    }
}
