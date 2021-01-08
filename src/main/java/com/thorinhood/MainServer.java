package com.thorinhood;

import com.thorinhood.utils.ArgumentParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class MainServer {

    private static final Logger log = LogManager.getLogger(MainServer.class);

    public static final String BASE_PATH = "basePath";
    public static final String PORT = "port";

    public static void main(String[] args) throws Exception {
//        try {
//            MinioClient minioClient =
//                    MinioClient.builder()
//                            .endpoint("http://127.0.0.1:9000")
//                            .credentials("AKIAIOSFODNN7EXAMPLE",
//                                          "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
//                            .build();
//
//            boolean found =
//                    minioClient.bucketExists(BucketExistsArgs.builder().bucket("test").build());
//            if (!found) {
//                minioClient.makeBucket(MakeBucketArgs.builder().bucket("test").build());
//            } else {
//                System.out.println("Bucket 'test' already exists.");
//            }
//
//            // Upload '/home/user/Photos/asiaphotos.zip' as object name 'asiaphotos-2015.zip' to bucket
//            // 'asiatrip'.
//            GetObjectResponse response = minioClient.getObject(GetObjectArgs.builder().bucket("test").object("pid").build());
//            StatObjectResponse responseStat = minioClient.statObject(StatObjectArgs.builder().bucket("test").object("pid").build());
//            int a = 5;
////            minioClient.uploadObject(
////                    UploadObjectArgs.builder()
////                            .bucket("test")
////                            .object("pid")
////                            .userMetadata(Map.of("kek", "kek"))
////                            .filename("/home/thorinhood/pid")
////                            .build());
////            System.out.println(
////                    "'~/s3.rb' is successfully uploaded as "
////                        + "object 's3.rb' to bucket 'test'.");
//        } catch (MinioException e) {
//            System.out.println("Error occurred: " + e);
//        }
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
        Server server = new Server(port, parsedArgs.get(BASE_PATH));
        log.info("port : {}", port);
        log.info("base path : {}", parsedArgs.get(BASE_PATH));
        server.run();
    }

}
