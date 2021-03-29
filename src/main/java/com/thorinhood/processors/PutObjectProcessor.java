package com.thorinhood.processors;

import com.thorinhood.data.S3Object;
import com.thorinhood.data.S3Util;
import com.thorinhood.exceptions.S3Exception;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.multipart.*;
import com.thorinhood.utils.DateTimeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class PutObjectProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(PutObjectProcessor.class);

    private HttpPostRequestDecoder httpDecoder;
    private final HttpDataFactory factory = new DefaultHttpDataFactory(true);

    public PutObjectProcessor(String basePath, HttpPostRequestDecoder httpDecoder) {
        super(basePath);
        this.httpDecoder = httpDecoder;
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, Object[] arguments)
            throws Exception {
        httpDecoder = new HttpPostRequestDecoder(factory, request);

//        List<InterfaceHttpData> datas = httpDecoder.getBodyHttpDatas();
        httpDecoder.offer(request);
        readChunk(context);

        String bucket = extractBucketPath(request);
        String key = extractKeyPath(request);
        try {
            S3Object s3Object = S3Util.putObject(bucket, key, BASE_PATH, request.content());
            if (s3Object == null) {
                sendError(context, INTERNAL_SERVER_ERROR, request);
                return;
            }
            sendResponseWithoutContent(context, OK, request, Map.of(
                    "ETag", s3Object.getETag(),
                    "Last-Modified", s3Object.getLastModified(),
                    "Date", DateTimeUtil.currentDateTime()
            ));
        } catch (S3Exception s3Exception) {
            sendError(context, request, s3Exception);
            log.error(s3Exception.getMessage(), s3Exception);
            return;
        }


//        for (InterfaceHttpData data : datas) {
//            if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.FileUpload) {
//                DiskFileUpload fileUpload = (DiskFileUpload) data;
//
//
//            }
//        }
    }


    private void readChunk(ChannelHandlerContext ctx) throws IOException {
        while (httpDecoder.hasNext()) {
            InterfaceHttpData data = httpDecoder.next();
            if (data != null) {
                try {
                    switch (data.getHttpDataType()) {
                        case Attribute:
                            break;
                        case FileUpload:
                            final FileUpload fileUpload = (FileUpload) data;
                            //final File file = new File(FILE_UPLOAD_LOCN + fileUpload.getFilename());
//                            if (!file.exists()) {
//                                file.createNewFile();
//                            }
//                            System.out.println("Created file " + file);
//                            try (FileChannel inputChannel = new FileInputStream(fileUpload.getFile()).getChannel();
//                                 FileChannel outputChannel = new FileOutputStream(file).getChannel()) {
//                                outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
//                                sendResponse(ctx, CREATED, "file name: " + file.getAbsolutePath());
//                            }
                            break;
                    }
                } finally {
                   // data.release();
                }
            }
        }
    }

}
