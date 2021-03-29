package com.thorinhood;

import com.thorinhood.db.H2DB;
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

    public static void main(String[] args) throws Exception {
        Map<String, String> parsedArgs = ArgumentParser.parseArguments(args);
        Set<String> missingArguments = ArgumentParser.checkArguments(parsedArgs, BASE_PATH, PORT, DB_PATH, DB_USER,
                DB_PASSWORD);
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
        H2DB h2Db = H2DB.getInstance(parsedArgs.get(DB_PATH), parsedArgs.get(DB_USER), parsedArgs.get(DB_PASSWORD));
        h2Db.init();
        Server server = new Server(port, parsedArgs.get(BASE_PATH), h2Db);
        log.info("port : {}", port);
        log.info("base path : {}", parsedArgs.get(BASE_PATH));
        log.info("database path : {}", parsedArgs.get(DB_PATH));
        server.run();
    }

}
