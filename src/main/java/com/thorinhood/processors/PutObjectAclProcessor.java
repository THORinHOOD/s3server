package com.thorinhood.processors;

import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.utils.S3Driver;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class PutObjectAclProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(PutObjectAclProcessor.class);

    public PutObjectAclProcessor(String basePath, S3Driver s3Driver) {
        super(basePath, s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        try {
            String lastModified = S3_DRIVER.putObjectAcl(BASE_PATH, parsedRequest.getBucket(), parsedRequest.getKey(),
                    parsedRequest.getBytes());
            sendResponseWithoutContent(context, OK, request, Map.of(
                    "Last-Modified", lastModified,
                    "Date", DateTimeUtil.currentDateTime(),
                    "Content-Length", 0
            ));
        } catch (S3Exception s3Exception) {
            sendError(context, request, s3Exception);
            log.error(s3Exception.getMessage(), s3Exception);
        }
    }

}
