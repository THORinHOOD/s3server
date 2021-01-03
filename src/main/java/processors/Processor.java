package processors;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public abstract class Processor {

    protected void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, FullHttpRequest request) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

        sendAndCleanupConnection(ctx, response, request);
    }

    protected void sendAndCleanupConnection(ChannelHandlerContext ctx, FullHttpResponse response,
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

    protected void setContentTypeHeader(HttpResponse response, File file) throws IOException {
        Path path = file.toPath();
        String mimeType = Files.probeContentType(path);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, mimeType);
    }

    public void process(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        if (!request.decoderResult().isSuccess()) {
            sendError(context, BAD_REQUEST, request);
        }
        processInner(context, request);
    }

    public abstract boolean isThisProcessor(FullHttpRequest request);
    protected abstract void processInner(ChannelHandlerContext context, FullHttpRequest request) throws Exception;
}
