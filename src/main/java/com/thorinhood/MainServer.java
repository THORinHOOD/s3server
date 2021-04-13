package com.thorinhood;

import com.thorinhood.drivers.*;
import com.thorinhood.utils.ArgumentParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class MainServer {

    private static final Logger log = LogManager.getLogger(MainServer.class);

    public static final String BASE_PATH = "basePath";
    public static final String PORT = "port";
    public static final String DB_USER = "dbUser";
    public static final String DB_PASSWORD = "dbPassword";
    public static final String DB_PATH = "dbPath";
    public static final String DB_TYPE = "dbType";

    public static void main(String[] args) throws Exception {
        Map<String, String> parsedArgs = ArgumentParser.parseArguments(args);
        Set<String> missingArguments = ArgumentParser.checkArguments(parsedArgs, BASE_PATH, PORT, DB_PATH, DB_USER,
                DB_PASSWORD, DB_TYPE);
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
        MetadataDriver metadataDriver = metadataDriverInit(parsedArgs);
        AclDriver aclDriver = aclDriverInit();
        Server server = new Server(port, parsedArgs.get(BASE_PATH), metadataDriver, aclDriver);
        log.info("port : {}", port);
        log.info("base path : {}", parsedArgs.get(BASE_PATH));
        server.run();
    }

    private static MetadataDriver metadataDriverInit(Map<String, String> parsedArgs) throws Exception {
        if (parsedArgs.get(DB_TYPE).equals("h2")) {
            H2Driver h2Driver = H2Driver.getInstance(parsedArgs.get(DB_PATH), parsedArgs.get(DB_USER), parsedArgs.get(DB_PASSWORD));
            h2Driver.init();
            log.info("database type : h2");
            log.info("database path : {}", parsedArgs.get(DB_PATH));
            return h2Driver;
        } else if (parsedArgs.get(DB_TYPE).equals("files")) {
            FileMetadataDriver fileMetadataDriver = new FileMetadataDriver();
            fileMetadataDriver.init();
            log.info("database type : files");
            return fileMetadataDriver;
        } else {
            throw new Exception("Wrong value dbType flag");
        }
    }

    private static AclDriver aclDriverInit() {
        return new FileAclDriver();
    }

}
