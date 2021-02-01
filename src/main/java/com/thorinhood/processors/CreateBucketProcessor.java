package com.thorinhood.processors;

import com.thorinhood.data.S3Util;
import com.thorinhood.exceptions.S3Exception;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.XmlUtil;

import java.io.File;
import java.util.Map;
import java.util.Optional;

public class CreateBucketProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(CreateBucketProcessor.class);

    public CreateBucketProcessor(String basePath) {
        super(basePath);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, Object[] arguments)
            throws Exception {
        String bucket = extractBucket(request);
        try {
            S3Util.createBucket(bucket, BASE_PATH);
            sendResponseWithoutContent(context, HttpResponseStatus.OK, request, Map.of(
                "Date", DateTimeUtil.currentDateTime(),
                "Location", File.separatorChar + bucket
            ));
        } catch (S3Exception exception) {
            sendError(context, request, exception);
            log.error(exception.getMessage(), exception);
        }
    }
}
