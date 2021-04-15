package com.thorinhood.processors.actions;

import com.thorinhood.chunks.ChunkReader;
import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class PutObjectProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(PutObjectProcessor.class);

    public PutObjectProcessor(String basePath, S3Driver s3Driver) {
        super(basePath, s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object[] arguments) throws Exception {
        if (!S3_DRIVER.checkBucketPermission(BASE_PATH, parsedRequest.getBucket(), METHOD_NAME,
                parsedRequest.getS3User())) {
            throw S3Exception.ACCESS_DENIED()
                    .setResource("1")
                    .setRequestId("1");
        }
        if (parsedRequest.getPayloadSignType() == PayloadSignType.SINGLE_CHUNK ||
            parsedRequest.getPayloadSignType() == PayloadSignType.UNSIGNED_PAYLOAD) {
            singleChunkRead(parsedRequest, request, context);
        } else {
            multipleChunksRead(parsedRequest, request, context);
        }
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    private void multipleChunksRead(ParsedRequest parsedRequest, FullHttpRequest request,
                                    ChannelHandlerContext context) {
        byte[] result = ChunkReader.readChunks(request, parsedRequest);
        parsedRequest.setBytes(result);
        singleChunkRead(parsedRequest, request, context);
    }

    private void singleChunkRead(ParsedRequest parsedRequest, FullHttpRequest request, ChannelHandlerContext context)
        throws S3Exception {
            S3Object s3Object = S3_DRIVER.putObject(
                    parsedRequest.getBucket(),
                    parsedRequest.getKey(),
                    BASE_PATH,
                    parsedRequest.getBytes(),
                    parsedRequest.getMetadata(),
                    parsedRequest.getS3User());

            if (s3Object == null) {
                sendError(context, INTERNAL_SERVER_ERROR, request);
                return;
            }
            sendResponseWithoutContent(context, OK, request, Map.of(
                    "ETag", s3Object.getETag(),
                    "Last-Modified", s3Object.getLastModified(),
                    "Date", DateTimeUtil.currentDateTime()
            ));
    }
}
