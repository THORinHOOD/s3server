package com.thorinhood.utils.utils;

import com.thorinhood.drivers.FileDriversFactory;
import com.thorinhood.drivers.acl.AclDriver;
import com.thorinhood.drivers.entity.EntityDriver;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.drivers.main.S3DriverImpl;
import com.thorinhood.drivers.metadata.MetadataDriver;
import com.thorinhood.drivers.principal.PolicyDriver;
import com.thorinhood.drivers.user.UserDriver;

public class TestServer {

    public static void start(String basePath, int port) throws Exception {
        FileDriversFactory fileFactory = new FileDriversFactory(basePath);
        fileFactory.init();
        UserDriver userDriver = fileFactory.createUserDriver();
        AclDriver aclDriver = fileFactory.createAclDriver();
        MetadataDriver metadataDriver = fileFactory.createMetadataDriver();
        PolicyDriver policyDriver = fileFactory.createPolicyDriver();
        EntityDriver entityDriver = fileFactory.createEntityDriver();
        S3Driver s3Driver = new S3DriverImpl(metadataDriver, aclDriver, policyDriver, entityDriver);
        com.thorinhood.Server server = new com.thorinhood.Server(port, s3Driver, userDriver);
        server.run();
    }

}
