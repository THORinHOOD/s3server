package com.thorinhood.handlers;

import com.thorinhood.drivers.AclDriver;
import com.thorinhood.drivers.MetadataDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.processors.*;
import com.thorinhood.processors.acl.GetObjectAclProcessor;
import com.thorinhood.processors.acl.PutBucketAclProcessor;
import com.thorinhood.processors.acl.PutObjectAclProcessor;
import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.utils.RequestUtil;
import com.thorinhood.utils.S3Driver;
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

    private final GetObjectProcessor getObjectProcessor;
    private final CreateBucketProcessor createBucketProcessor;
    private final PutObjectProcessor putObjectProcessor;
    private final PutObjectAclProcessor putObjectAclProcessor;
    private final PutBucketAclProcessor putBucketAclProcessor;
    private final GetObjectAclProcessor getObjectAclProcessor;

    public ServerHandler(String basePath, MetadataDriver metadataDriver, AclDriver aclDriver) {
        S3Driver s3Driver = new S3Driver(metadataDriver, aclDriver);
        getObjectProcessor = new GetObjectProcessor(basePath, s3Driver);
        createBucketProcessor = new CreateBucketProcessor(basePath, s3Driver);
        putObjectProcessor = new PutObjectProcessor(basePath, s3Driver);
        putObjectAclProcessor = new PutObjectAclProcessor(basePath, s3Driver);
        putBucketAclProcessor = new PutBucketAclProcessor(basePath, s3Driver);
        getObjectAclProcessor = new GetObjectAclProcessor(basePath, s3Driver);
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

        String secretKey = "m+I32QXn2PPwpb6JyMO96qoKAeRbfOknY80GenIm"; // TODO


        ParsedRequest parsedRequest = RequestUtil.parseRequest(request);
        try {
            RequestUtil.checkRequest(request, parsedRequest, secretKey);
        } catch (S3Exception exception) {
            getObjectProcessor.sendError(context, request, exception); // TODO
            return false;
        }

        if (request.method().equals(HttpMethod.GET)) {
            if (isAclRequest(request)) {
                if (!parsedRequest.getKey().equals("")) {
                    getObjectAclProcessor.process(context, request, parsedRequest);
                } else {
//                    putBucketAclProcessor.process(context, request, parsedRequest); // TODO
                }
                return true;
            }
        }

        if (request.method().equals(HttpMethod.GET)) {
            getObjectProcessor.process(context, request, parsedRequest);
            return true;
        }

        if (request.method().equals(HttpMethod.PUT)) {
            if (isAclRequest(request)) {
                if (!parsedRequest.getKey().equals("")) {
                    putObjectAclProcessor.process(context, request, parsedRequest);
                } else {
                    putBucketAclProcessor.process(context, request, parsedRequest);
                }
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
            putObjectProcessor.process(context, request, parsedRequest, secretKey);
            return true;
        }
        return false;
    }

    private boolean isAclRequest(FullHttpRequest request) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri());
        Map<String, List<String>> params = queryStringDecoder.parameters();
        return params.containsKey("acl");
    }

}
