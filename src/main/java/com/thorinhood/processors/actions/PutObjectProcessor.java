package com.thorinhood.processors.actions;

import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.utils.PayloadSignType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class PutObjectProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(PutObjectProcessor.class);

    public PutObjectProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object[] arguments) throws Exception {
        checkRequestPermissions(parsedRequest, true);
        if (parsedRequest.getPayloadSignType() != PayloadSignType.SINGLE_CHUNK &&
            parsedRequest.getPayloadSignType() != PayloadSignType.UNSIGNED_PAYLOAD) {
            parsedRequest.setBytes(processChunkedContent(parsedRequest, request));
        }
        S3Object s3Object = S3_DRIVER.putObject(
                parsedRequest.getS3ObjectPath(),
                parsedRequest.getBytes(),
                parsedRequest.getMetadata(),
                parsedRequest.getS3User());
        sendResponseWithoutContent(context, OK, request, Map.of(
                "ETag", s3Object.getETag(),
                "Last-Modified", s3Object.getLastModified(),
                "Date", DateTimeUtil.currentDateTime()
        ));
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

}
