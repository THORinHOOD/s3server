package com.thorinhood;

import com.thorinhood.drivers.FileDriver;
import com.thorinhood.drivers.FileDriversFactory;
import com.thorinhood.drivers.acl.AclDriver;
import com.thorinhood.drivers.entity.EntityDriver;
import com.thorinhood.drivers.lock.EntityLocker;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.drivers.main.S3FileDriverImpl;
import com.thorinhood.drivers.metadata.MetadataDriver;
import com.thorinhood.drivers.principal.PolicyDriver;
import com.thorinhood.drivers.user.UserDriver;
import com.thorinhood.utils.ArgumentParser;
import com.thorinhood.utils.RequestUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class MainServer {

    private static final Logger log = LogManager.getLogger(MainServer.class);

    public static final String BASE_PATH = "basePath";
    public static final String PORT = "port";
    public static final String USERS = "users";

    public static void main(String[] args) throws Exception {
        Map<String, String> parsedArgs = ArgumentParser.parseArguments(args);
        Set<String> missingArguments = ArgumentParser.checkArguments(parsedArgs, BASE_PATH, PORT);
        if (!missingArguments.isEmpty()) {
            missingArguments.forEach(key -> System.out.println("Missing argument : " + key));
            return;
        }
        int port;
        try {
            port = Integer.parseInt(parsedArgs.get(PORT));
        } catch (NumberFormatException exception) {
            log.error("\'--port\' is not int");
            return;
        }

        EntityLocker entityLocker = new EntityLocker();
        FileDriversFactory fileFactory = new FileDriversFactory(parsedArgs.get(BASE_PATH));
        try {
            fileFactory.init();
        } catch (Exception exception) {
            log.error(exception.getMessage());
            return;
        }
        UserDriver userDriver = fileFactory.createUserDriver();
        AclDriver aclDriver = fileFactory.createAclDriver();
        MetadataDriver metadataDriver = fileFactory.createMetadataDriver();
        PolicyDriver policyDriver = fileFactory.createPolicyDriver();
        EntityDriver entityDriver = fileFactory.createEntityDriver();
        FileDriver fileDriver = fileFactory.createFileDriver();
        S3Driver s3Driver = new S3FileDriverImpl(metadataDriver, aclDriver, policyDriver, entityDriver, fileDriver,
                entityLocker);
        if (parsedArgs.containsKey(USERS)) {
            addAllRootUsers(userDriver, parsedArgs.get(USERS));
        }
        Server server = new Server(port, s3Driver, new RequestUtil(userDriver, parsedArgs.get(BASE_PATH)));
        log.info("port : {}", port);
        log.info("base path : {}", parsedArgs.get(BASE_PATH));
        server.run();
    }

    private static void addAllRootUsers(UserDriver userDriver, String files) throws Exception {
        if (files == null || files.isEmpty() || files.isBlank()) {
            throw new Exception(USERS + " the parameter must contain the paths to the files with users");
        }
        for (String s : files.split(",")) {
            userDriver.addUser(s);
        }
    }

}
