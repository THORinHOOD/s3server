package processors;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class GetObjectProcessor extends Processor {

    private final String baseBucketPath;

    public GetObjectProcessor(String baseBucketPath) {
        this.baseBucketPath = baseBucketPath;
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
        final String uri = request.uri();
        final String path = buildPath(baseBucketPath, uri);
        if (path == null) {
            sendError(context, FORBIDDEN, request);
            return;
        }
        final boolean keepAlive = HttpUtil.isKeepAlive(request);

        System.out.println(path);

        File file = new File(path);
        if (file.isHidden() || !file.exists()) {
            this.sendError(context, NOT_FOUND, request);
            return;
        }
        if (!file.isFile()) {
            this.sendError(context, FORBIDDEN, request);
            return;
        }
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException ignore) {
            sendError(context, NOT_FOUND, request);
            return;
        }
        long fileLength = raf.length();
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpUtil.setContentLength(response, fileLength);
        setContentTypeHeader(response, file);

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
    }

    private static String buildPath(String basePath, String uri) {
        uri = URLDecoder.decode(uri, StandardCharsets.UTF_8);
        if (uri.isEmpty() || uri.charAt(0) != '/') {
            return null;
        }
        uri = uri.replace('/', File.separatorChar);
        return basePath + uri;
    }
}
