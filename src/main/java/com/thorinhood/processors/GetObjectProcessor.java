package com.thorinhood.processors;

import com.thorinhood.data.S3Object;
import com.thorinhood.data.S3Util;
import com.thorinhood.exceptions.S3Exception;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import com.thorinhood.utils.DateTimeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class GetObjectProcessor extends Processor {

    private static final Logger log = LogManager.getLogger(GetObjectProcessor.class);

    public GetObjectProcessor(String basePath) {
        super(basePath);
    }

    @Override
    public void processInner(ChannelHandlerContext context, FullHttpRequest request, Object[] arguments) throws Exception {
        final boolean keepAlive = HttpUtil.isKeepAlive(request);
        String bucket = extractBucket(request);
        String key = extractKey(request);
        S3Object s3Object;
        try {
            s3Object = S3Util.getObject(bucket, key, BASE_PATH);
        } catch (S3Exception s3Exception) {
            sendError(context, request, s3Exception);
            log.error(s3Exception.getMessage(), s3Exception);
            return;
        }

        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(s3Object.getFile(), "r");
        } catch (FileNotFoundException ignore) {
            sendError(context, NOT_FOUND, request);
            return;
        }
        long fileLength = raf.length();
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpUtil.setContentLength(response, fileLength);
        setContentTypeHeader(response, s3Object.getFile());
        response.headers().set("ETag", s3Object.getETag());
        response.headers().set("Last-Modified", s3Object.getLastModified());
        response.headers().set("Date", DateTimeUtil.currentDateTime());

        if (!keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        }

        context.write(response);
        ChannelFuture sendFileFuture;
        ChannelFuture lastContentFuture;

        sendFileFuture = context.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength),
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

}
