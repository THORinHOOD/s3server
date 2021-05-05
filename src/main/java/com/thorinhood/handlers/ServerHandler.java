package com.thorinhood.handlers;

import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.drivers.user.UserDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.Processor;
import com.thorinhood.processors.acl.GetBucketAclProcessor;
import com.thorinhood.processors.acl.GetObjectAclProcessor;
import com.thorinhood.processors.acl.PutBucketAclProcessor;
import com.thorinhood.processors.acl.PutObjectAclProcessor;
import com.thorinhood.processors.actions.*;
import com.thorinhood.processors.lists.ListBucketsProcessor;
import com.thorinhood.processors.lists.ListObjectsV2Processor;
import com.thorinhood.processors.multipart.AbortMultipartUploadProcessor;
import com.thorinhood.processors.multipart.CreateMultipartUploadProcessor;
import com.thorinhood.processors.policies.GetBucketPolicyProcessor;
import com.thorinhood.processors.policies.PutBucketPolicyProcessor;
import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.utils.RequestUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

@ChannelHandler.Sharable
public class ServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger log = LogManager.getLogger(ServerHandler.class);

    private final RequestUtil requestUtil;
    private final GetObjectProcessor getObjectProcessor;
    private final CreateBucketProcessor createBucketProcessor;
    private final PutObjectProcessor putObjectProcessor;
    private final PutObjectAclProcessor putObjectAclProcessor;
    private final PutBucketAclProcessor putBucketAclProcessor;
    private final GetObjectAclProcessor getObjectAclProcessor;
    private final GetBucketAclProcessor getBucketAclProcessor;
    private final PutBucketPolicyProcessor putBucketPolicyProcessor;
    private final GetBucketPolicyProcessor getBucketPolicyProcessor;
    private final DeleteObjectProcessor deleteObjectProcessor;
    private final ListObjectsV2Processor listObjectsV2Processor;
    private final DeleteBucketProcessor deleteBucketProcessor;
    private final ListBucketsProcessor listBucketsProcessor;
    private final CreateMultipartUploadProcessor createMultipartUploadProcessor;
    private final AbortMultipartUploadProcessor abortMultipartUploadProcessor;

    public ServerHandler(S3Driver s3Driver, UserDriver userDriver) {
        requestUtil = new RequestUtil(userDriver);
        getObjectProcessor = new GetObjectProcessor(s3Driver);
        createBucketProcessor = new CreateBucketProcessor(s3Driver);
        putObjectProcessor = new PutObjectProcessor(s3Driver);
        putObjectAclProcessor = new PutObjectAclProcessor(s3Driver);
        putBucketAclProcessor = new PutBucketAclProcessor(s3Driver);
        getObjectAclProcessor = new GetObjectAclProcessor(s3Driver);
        getBucketAclProcessor = new GetBucketAclProcessor(s3Driver);
        putBucketPolicyProcessor = new PutBucketPolicyProcessor(s3Driver);
        getBucketPolicyProcessor = new GetBucketPolicyProcessor(s3Driver);
        deleteObjectProcessor = new DeleteObjectProcessor(s3Driver);
        listObjectsV2Processor = new ListObjectsV2Processor(s3Driver);
        deleteBucketProcessor = new DeleteBucketProcessor(s3Driver);
        listBucketsProcessor = new ListBucketsProcessor(s3Driver);
        createMultipartUploadProcessor = new CreateMultipartUploadProcessor(s3Driver);
        abortMultipartUploadProcessor = new AbortMultipartUploadProcessor(s3Driver);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        try {
            boolean processed = process(context, request);
            if (!processed) {
                log.error("Not found any processor for request or error occurred");
            }
            //TODO
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error(cause);
        ctx.channel().close();
    }

    private boolean process(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        ParsedRequest parsedRequest = null;
        try {
            parsedRequest = requestUtil.parseRequest(request);
            requestUtil.checkRequest(parsedRequest);
        } catch (S3Exception exception) {
            Processor.sendError(context, request, exception);
            return false;
        }

        if (request.method().equals(HttpMethod.POST)) {
            if (checkRequestS3Type(request, "uploads")) {
                createMultipartUploadProcessor.process(context, request, parsedRequest);
            }
            return true;
        }

        if (request.method().equals(HttpMethod.GET)) {
            if (isAclRequest(request)) {
                if (parsedRequest.isPathToObject()) {
                    getObjectAclProcessor.process(context, request, parsedRequest);
                } else {
                    getBucketAclProcessor.process(context, request, parsedRequest);
                }
            } else if (isPolicyRequest(request)) {
                getBucketPolicyProcessor.process(context, request, parsedRequest);
            } else if (!parsedRequest.isPathToObject() && checkRequestS3Type(request, "list-type", "2")) {
                listObjectsV2Processor.process(context, request, parsedRequest);
            } else if (parsedRequest.isPathToObject()) {
                getObjectProcessor.process(context, request, parsedRequest);
            } else {
                listBucketsProcessor.process(context, request, parsedRequest);
            }
            return true;
        }

        if (request.method().equals(HttpMethod.PUT)) {
            if (isAclRequest(request)) {
                if (parsedRequest.isPathToObject()) {
                    putObjectAclProcessor.process(context, request, parsedRequest);
                } else {
                    putBucketAclProcessor.process(context, request, parsedRequest);
                }
                return true;
            } else if (isPolicyRequest(request)) {
                putBucketPolicyProcessor.process(context, request, parsedRequest);
                return true;
            }
        }

        if (request.method().equals(HttpMethod.PUT)) {
            if (parsedRequest.isPathToObject()) {
                putObjectProcessor.process(context, request, parsedRequest);
            } else {
                createBucketProcessor.process(context, request, parsedRequest);
            }
        }

        if (request.method().equals(HttpMethod.DELETE)) {
            if (parsedRequest.isPathToObject() && checkRequestS3Type(request, "uploadId")) {
                abortMultipartUploadProcessor.process(context, request, parsedRequest);
            } else if (parsedRequest.isPathToObject()) {
                deleteObjectProcessor.process(context, request, parsedRequest);
            } else {
                deleteBucketProcessor.process(context, request, parsedRequest);
            }
            return true;
        }

        return false;
    }

    private boolean isPolicyRequest(FullHttpRequest request) {
        return checkRequestS3Type(request, "policy");
    }

    private boolean isAclRequest(FullHttpRequest request) {
        return checkRequestS3Type(request, "acl");
    }

    private boolean checkRequestS3Type(FullHttpRequest request, String key, String value) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
        Map<String, List<String>> params = queryStringDecoder.parameters();
        return params.containsKey(key) && params.get(key) != null && params.get(key).size() == 1 &&
                params.get(key).get(0).equals(value);
    }

    private boolean checkRequestS3Type(FullHttpRequest request, String type) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
        Map<String, List<String>> params = queryStringDecoder.parameters();
        return params.containsKey(type);
    }
}
