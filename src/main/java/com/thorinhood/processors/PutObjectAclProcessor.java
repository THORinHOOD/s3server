package com.thorinhood.processors;

import com.thorinhood.acl.AccessControlPolicy;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.utils.PayloadSignType;
import com.thorinhood.utils.RequestUtil;
import com.thorinhood.utils.S3Util;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PutObjectAclProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(PutObjectAclProcessor.class);

    public PutObjectAclProcessor(String basePath, S3Util s3Util) {
        super(basePath, s3Util);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, Object... arguments)
            throws Exception {
        try {
            String secretKey = "m+I32QXn2PPwpb6JyMO96qoKAeRbfOknY80GenIm"; // TODO

            ParsedRequest parsedRequest = RequestUtil.parseRequest(request);
//            RequestUtil.checkRequest(request, parsedRequest, secretKey);

            S3_UTIL.putObjectAcl(BASE_PATH, parsedRequest.getBucket(), parsedRequest.getKey(), parsedRequest.getBytes());
        } catch (S3Exception s3Exception) {
            sendError(context, request, s3Exception);
            log.error(s3Exception.getMessage(), s3Exception);
        }
    }

}
