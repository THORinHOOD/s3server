package com.thorinhood.processors.multipart;

import com.thorinhood.data.multipart.CompleteMultipartUpload;
import com.thorinhood.data.results.CompleteMultipartUploadResult;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import com.thorinhood.utils.XmlUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;

import java.util.function.Function;

public class CompleteMultipartUploadProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(CompleteMultipartUploadProcessor.class);


    public CompleteMultipartUploadProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                                Object... arguments) throws Exception {
        checkRequest(parsedRequest, "s3:PutObject", true);
        Document document = XmlUtil.parseXmlFromBytes(parsedRequest.getBytes());
        CompleteMultipartUpload completeMultipartUpload = CompleteMultipartUpload
                .buildFromNode(document.getDocumentElement());
        String eTag = S3_DRIVER.completeMultipartUpload(
                parsedRequest.getS3ObjectPath(),
                parsedRequest.getQueryParam("uploadId", null, Function.identity()),
                completeMultipartUpload.getParts(),
                parsedRequest.getS3User());
        CompleteMultipartUploadResult completeMultipartUploadResult = CompleteMultipartUploadResult.builder()
                .setETag(eTag)
                .setKey(parsedRequest.getS3ObjectPath().getKey())
                .setLocation(parsedRequest.getS3ObjectPath().getKeyWithBucket())
                .setBucket(parsedRequest.getS3ObjectPath().getBucket())
                .build();
        String xml = completeMultipartUploadResult.buildXmlText();
        sendResponse(context, request, HttpResponseStatus.OK, response -> {
            response.headers().set("Date", DateTimeUtil.currentDateTime());
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, xml.getBytes().length);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml");
        }, xml);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
