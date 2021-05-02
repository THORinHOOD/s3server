package com.thorinhood.processors.actions;

import com.thorinhood.data.s3object.S3Object;
import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.processors.Processor;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.ParsedRequest;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class GetObjectProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(GetObjectProcessor.class);

    public GetObjectProcessor(S3Driver s3Driver) {
        super(s3Driver);
    }

    @Override
    public void processInner(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                             Object[] arguments) throws Exception {
        checkRequest(parsedRequest, false);

        final boolean keepAlive = HttpUtil.isKeepAlive(request);

        S3Object s3Object = S3_DRIVER.getObject(parsedRequest.getS3ObjectPath(), parsedRequest.getHeaders());

        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(s3Object.getFile(), "r");
        } catch (FileNotFoundException exception) {
            sendError(context, NOT_FOUND, request);
            log.error(exception.getMessage(), exception);
            return;
        }
        long fileLength = raf.length();
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpUtil.setContentLength(response, fileLength);
        setContentTypeHeader(response, s3Object.getFile());
        response.headers().set("ETag", s3Object.getETag());
        response.headers().set("Last-Modified", s3Object.getLastModified());
        response.headers().set("Date", DateTimeUtil.currentDateTime());
        s3Object.getMetaData().forEach((metaKey, metaValue) ->
                response.headers().set("x-amz-meta-" + metaKey, metaValue));

        context.write(response);
        ChannelFuture sendFileFuture;
        ChannelFuture lastContentFuture;

        sendFileFuture = context.write(new DefaultFileRegion(raf.getChannel(), 0, s3Object.getFile().length()),
                context.newProgressivePromise());
        lastContentFuture = context.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

        sendFileFuture.addListener(new ChannelProgressiveFutureListener() {
            @Override
            public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
                if (total < 0) {
                    log.info(future.channel() + " Transfer progress: " + progress);
                } else {
                    log.info(future.channel() + " Transfer progress: " + progress + " / " + total);
                }
            }

            @Override
            public void operationComplete(ChannelProgressiveFuture future) {
                log.info(future.channel() + " Transfer complete.");
            }
        });

        if (!keepAlive) {
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }

    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
