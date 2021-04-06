package com.thorinhood.handlers;

import com.thorinhood.data.S3Util;
import com.thorinhood.db.H2DB;
import com.thorinhood.utils.XmlUtil;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.*;
import com.thorinhood.processors.*;
import io.netty.handler.stream.ChunkedStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@ChannelHandler.Sharable
public class ServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger log = LogManager.getLogger(ServerHandler.class);

    private final GetObjectProcessor getObjectProcessor;
    private final CreateBucketProcessor createBucketProcessor;
    private final PutObjectProcessor putObjectProcessor;

    public ServerHandler(String basePath, H2DB h2Db) {
        S3Util s3Util = new S3Util(h2Db);
        getObjectProcessor = new GetObjectProcessor(basePath, s3Util);
        createBucketProcessor = new CreateBucketProcessor(basePath, s3Util);
        putObjectProcessor = new PutObjectProcessor(basePath, s3Util);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        try {
            boolean processed = process(context, request);
            if (!processed) {
                log.error("Not found any processor for request");
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
        if (request.method().equals(HttpMethod.GET)) {
            getObjectProcessor.process(context, request);
            return true;
        }
        if (request.method().equals(HttpMethod.PUT)) {
            Optional<Document> content = XmlUtil.parseXmlFromByteBuf(request.content());
            if (content.isPresent()) {
                NodeList nodeList = content.get().getChildNodes();
                if (nodeList.getLength() == 1 && nodeList.item(0).getNodeName()
                        .equals("CreateBucketConfiguration")) {
                    createBucketProcessor.process(context, request, content.get());
                    return true;
                }
            }
        }
        if (request.method().equals(HttpMethod.PUT)) {
            putObjectProcessor.process(context, request);
            return true;
        }
        return false;
    }

}
