package com.thorinhood.processors;

import com.thorinhood.chunks.ChunkReader;
import com.thorinhood.data.*;
import com.thorinhood.drivers.S3Driver;
import com.thorinhood.exceptions.S3Exception;
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
        try {
            if (parsedRequest.getPayloadSignType() == PayloadSignType.SINGLE_CHUNK ||
                parsedRequest.getPayloadSignType() == PayloadSignType.UNSIGNED_PAYLOAD) {
                singleChunkRead(parsedRequest, request, context);
            } else {
                multipleChunksRead(parsedRequest, request, context, (String) arguments[0]);
            }
        } catch (S3Exception s3Exception) {
            sendError(context, request, s3Exception);
            log.error(s3Exception.getMessage(), s3Exception);
        }
    }

    private void multipleChunksRead(ParsedRequest parsedRequest, FullHttpRequest request, ChannelHandlerContext context,
                                    String secretKey) {
        byte[] result = ChunkReader.readChunks(request, parsedRequest, secretKey);
        parsedRequest.setBytes(result);
        singleChunkRead(parsedRequest, request, context);
    }

    private void singleChunkRead(ParsedRequest parsedRequest, FullHttpRequest request, ChannelHandlerContext context)
        throws S3Exception {
            S3Object s3Object = S3_DRIVER.putObject(
                    parsedRequest.getBucket(),
                    parsedRequest.getKey(),
                    BASE_PATH,
                    parsedRequest.getBytes(), RequestUtil.extractMetaData(request));

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
