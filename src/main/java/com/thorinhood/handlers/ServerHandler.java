package com.thorinhood.handlers;

import com.thorinhood.data.S3User;
import com.thorinhood.drivers.config.ConfigDriver;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.*;
import com.thorinhood.processors.acl.GetBucketAclProcessor;
import com.thorinhood.processors.acl.GetObjectAclProcessor;
import com.thorinhood.processors.acl.PutBucketAclProcessor;
import com.thorinhood.processors.acl.PutObjectAclProcessor;
import com.thorinhood.processors.actions.CreateBucketProcessor;
import com.thorinhood.processors.actions.GetObjectProcessor;
import com.thorinhood.processors.actions.PutObjectProcessor;
import com.thorinhood.processors.policies.GetBucketPolicyProcessor;
import com.thorinhood.processors.policies.PutBucketPolicyProcessor;
import com.thorinhood.utils.Credential;
import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.utils.RequestUtil;
import com.thorinhood.utils.XmlUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    public ServerHandler(String basePath, S3Driver s3Driver, ConfigDriver configDriver) {
        requestUtil = new RequestUtil(configDriver);
        getObjectProcessor = new GetObjectProcessor(basePath, s3Driver);
        createBucketProcessor = new CreateBucketProcessor(basePath, s3Driver);
        putObjectProcessor = new PutObjectProcessor(basePath, s3Driver);
        putObjectAclProcessor = new PutObjectAclProcessor(basePath, s3Driver);
        putBucketAclProcessor = new PutBucketAclProcessor(basePath, s3Driver);
        getObjectAclProcessor = new GetObjectAclProcessor(basePath, s3Driver);
        getBucketAclProcessor = new GetBucketAclProcessor(basePath, s3Driver);
        putBucketPolicyProcessor = new PutBucketPolicyProcessor(basePath, s3Driver);
        getBucketPolicyProcessor = new GetBucketPolicyProcessor(basePath, s3Driver);
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
        ParsedRequest parsedRequest = requestUtil.parseRequest(request);
        try {
            requestUtil.checkRequest(parsedRequest);
        } catch (S3Exception exception) {
            Processor.sendError(context, request, exception);
            return false;
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
            } else {
                getObjectProcessor.process(context, request, parsedRequest);
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
            Optional<Document> content = XmlUtil.parseXmlFromByteBuf(request.content());
            if (content.isPresent()) {
                NodeList nodeList = content.get().getChildNodes();
                if (nodeList.getLength() == 1 && nodeList.item(0).getNodeName()
                        .equals("CreateBucketConfiguration")) {
                    createBucketProcessor.process(context, request, parsedRequest, content.get());
                    return true;
                }
            }
        }

        if (request.method().equals(HttpMethod.PUT)) {
            putObjectProcessor.process(context, request, parsedRequest);
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

    private boolean checkRequestS3Type(FullHttpRequest request, String type) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
        Map<String, List<String>> params = queryStringDecoder.parameters();
        return params.containsKey(type);
    }
}
