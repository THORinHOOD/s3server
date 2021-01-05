package processors;

import data.S3Object;
import exceptions.S3Exception;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import utils.DateTimeUtil;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class GetObjectProcessor extends Processor {

    public GetObjectProcessor(String basePath) {
        super(basePath);
    }

    @Override
    public boolean isThisProcessor(FullHttpRequest request) {
        if (!request.method().equals(HttpMethod.GET)) {
            return false;
        }
        return true;
    }

    @Override
    public void processInner(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        final boolean keepAlive = HttpUtil.isKeepAlive(request);
        String bucket = extractBucket(request);
        String key = extractKey(request);
        S3Object s3Object;
        try {
            s3Object = S3Object.get(bucket, key, BASE_PATH);
        } catch (S3Exception s3Exception) {
            sendError(context, request, s3Exception);
            s3Exception.printStackTrace();
            return;
        }
        System.out.println(s3Object.getAbsolutePath());

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
                    System.out.println(future.channel() + " Transfer progress: " + progress);
                } else {
                    System.out.println(future.channel() + " Transfer progress: " + progress + " / " + total);
                }
            }
            @Override
            public void operationComplete(ChannelProgressiveFuture future) {
                System.out.println(future.channel() + " Transfer complete.");
            }
        });

        if (!keepAlive) {
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
        //TODO NORMAL RESPONSE
    }

}
