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

    private final List<Processor> processors;
    private HttpPostRequestDecoder httpDecoder;

    private final S3Util s3Util;
    private final GetObjectProcessor getObjectProcessor;
    private final CreateBucketProcessor createBucketProcessor;
    private final PutObjectProcessor putObjectProcessor;

    public ServerHandler(String basePath, H2DB h2Db) {
        s3Util = new S3Util(h2Db);
        processors = new ArrayList<>();
        getObjectProcessor = new GetObjectProcessor(basePath, s3Util);
        createBucketProcessor = new CreateBucketProcessor(basePath, s3Util);
        putObjectProcessor = new PutObjectProcessor(basePath, s3Util);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        try {
//            httpDecoder = new HttpPostRequestDecoder(new DefaultHttpDataFactory(true), request);

            boolean processed = process(context, request);
            if (!processed) {
                log.error("Not found any processor for request");
            }
            //TODO
        } catch (Exception exception) {
            exception.printStackTrace();
        }


//        String content = request.content().toString(StandardCharsets.UTF_8);
//        ByteBufInputStream st = new ByteBufInputStream(request.content());
//        new InputStream(request.content())
//        HttpPostStandardRequestDecoder decoder = new HttpPostStandardRequestDecoder(new DefaultHttpDataFactory(true), request,
//                StandardCharsets.UTF_8);
//        boolean readingChunks = HttpUtil.isTransferEncodingChunked(request);
//        log.info("Is Chunked: " + readingChunks + "\r\n");
//        HttpPostRequestDecoder decoder = null;
//        try {
//            decoder = new HttpPostRequestDecoder(request);
//            for (InterfaceHttpData httpData : decoder.getBodyHttpDatas()) {
//                InterfaceHttpData.HttpDataType _type = httpData.getHttpDataType();
//                if (_type == InterfaceHttpData.HttpDataType.Attribute) {
////                    Attribute attribute = (Attribute) httpData;
////                    parseAttribute(attribute);
//                } else if (_type == InterfaceHttpData.HttpDataType.FileUpload) {
//                    FileUpload upload = (FileUpload) httpData;
////                    multipartFiles.add(MultipartFileFactory.create(upload));
//                }
//            }
//        } catch (Exception ex) {
//            log.warn(ex.getMessage());
//        } finally {
//            if (decoder != null)
//                decoder.destroy();
//        }

//        final HttpContent chunk = (HttpContent) request;
//        httpDecoder.offer(chunk);
//        httpDecoder.setDiscardThreshold(0);
//        readChunk(context);
//        if (chunk instanceof LastHttpContent) {
//            resetPostRequestDecoder((HttpRequest) request);
//        }
    }

//    private void readChunk(ChannelHandlerContext ctx) throws IOException {
//        while (httpDecoder.hasNext()) {
//            InterfaceHttpData data = httpDecoder.next();
//            if (data != null) {
//                try {
//                    switch (data.getHttpDataType()) {
//                        case Attribute:
//                            break;
//                        case FileUpload:
//                            final FileUpload fileUpload = (FileUpload) data;
//                            //final File file = new File(FILE_UPLOAD_LOCN + fileUpload.getFilename());
////                            if (!file.exists()) {
////                                file.createNewFile();
////                            }
////                            System.out.println("Created file " + file);
////                            try (FileChannel inputChannel = new FileInputStream(fileUpload.getFile()).getChannel();
////                                 FileChannel outputChannel = new FileOutputStream(file).getChannel()) {
////                                outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
////                                sendResponse(ctx, CREATED, "file name: " + file.getAbsolutePath());
////                            }
//                            break;
//                    }
//                } finally {
//                    // data.release();
//                }
//            }
//        }
//    }

    private void resetPostRequestDecoder(HttpRequest request) {
        request = null;
        httpDecoder.destroy();
        httpDecoder = null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error(cause);
        ctx.channel().close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (httpDecoder != null) {
            httpDecoder.cleanFiles();
        }
    }

    //cb07bd462d590e1590765740f637626a2788a5e47d70c573a59bbc00ca75358a
    //cb07bd462d590e1590765740f637626a2788a5e47d70c573a59bbc00ca75358a
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
