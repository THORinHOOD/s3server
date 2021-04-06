package com.thorinhood.processors;

import com.thorinhood.data.*;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.Credential;
import com.thorinhood.utils.RequestWorker;
import com.thorinhood.utils.SignChecker;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.*;
import com.thorinhood.utils.DateTimeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.signer.internal.Aws4SignerUtils;
import software.amazon.awssdk.auth.signer.internal.SigningAlgorithm;
import software.amazon.awssdk.utils.BinaryUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class PutObjectProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(PutObjectProcessor.class);

    private HttpPostRequestDecoder httpDecoder;
    private final HttpDataFactory factory = new DefaultHttpDataFactory(false);

    public PutObjectProcessor(String basePath, S3Util s3Util) {
        super(basePath, s3Util);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, Object[] arguments)
            throws Exception {
        try {
            PayloadSignType payloadSignType = RequestWorker.getPayloadSignType(request);

            String bucket = extractBucketPath(request);
            String key = extractKeyPath(request);

            ParsedRequest parsedRequest = RequestWorker.processRequest(payloadSignType, request, bucket, key);

            if (payloadSignType == PayloadSignType.SINGLE_CHUNK || payloadSignType == PayloadSignType.UNSIGNED_PAYLOAD) {
                    singleChunkRead(
                            parsedRequest.getBucket(),
                            parsedRequest.getKey(),
                            request,
                            context,
                            parsedRequest.getBytes());
            } else {
                //TODO chunked read

            }
        } catch(S3Exception s3Exception) {
            sendError(context, request, s3Exception);
            log.error(s3Exception.getMessage(), s3Exception);
        }
    }

    private void singleChunkRead(String bucket, String key, FullHttpRequest request, ChannelHandlerContext context,
                                 byte[] bytes)
            throws S3Exception {
            S3Object s3Object = S3_UTIL.putObject(bucket, key, BASE_PATH, bytes, RequestWorker.extractMetaData(request));
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
