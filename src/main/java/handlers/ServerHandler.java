package handlers;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.SystemPropertyUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class ServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        if (!request.decoderResult().isSuccess()) {
            sendError(context, BAD_REQUEST, request);
        }

        final boolean keepAlive = HttpUtil.isKeepAlive(request);
        final String uri = request.uri();
        final String path = sanitizeUri(uri);
        if (path == null) {
            this.sendError(context, FORBIDDEN, request);
            return;
        }
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

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, FullHttpRequest request) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

        this.sendAndCleanupConnection(ctx, response, request);
    }

    private static String sanitizeUri(String uri) {
        uri = URLDecoder.decode(uri, StandardCharsets.UTF_8);
        if (uri.isEmpty() || uri.charAt(0) != '/') {
            return null;
        }
        uri = uri.replace('/', File.separatorChar);
        return SystemPropertyUtil.get("user.dir") + uri;
    }

    private void sendAndCleanupConnection(ChannelHandlerContext ctx, FullHttpResponse response,
                                          FullHttpRequest request) {
        final boolean keepAlive = HttpUtil.isKeepAlive(request);
        HttpUtil.setContentLength(response, response.content().readableBytes());
        if (!keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        } else if (request.protocolVersion().equals(HTTP_1_0)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }
        ChannelFuture flushPromise = ctx.writeAndFlush(response);
        if (!keepAlive) {
            flushPromise.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static void setContentTypeHeader(HttpResponse response, File file) throws IOException {
        Path path = file.toPath();
        String mimeType = Files.probeContentType(path);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, mimeType);
    }
}
