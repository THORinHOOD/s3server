package com.thorinhood.processors;

import com.thorinhood.drivers.main.S3Driver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.ParsedRequest;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public abstract class Processor {

    protected final S3Driver S3_DRIVER;
    protected final String METHOD_NAME;

    public Processor(S3Driver s3Driver) {
        this.S3_DRIVER = s3Driver;
        METHOD_NAME = "s3:" + this.getClass().getSimpleName().substring(0,
                this.getClass().getSimpleName().indexOf("Processor"));
    }

    protected void checkRequest(ParsedRequest request, boolean isBucketAcl) throws S3Exception {
        checkRequest(request, METHOD_NAME, isBucketAcl);
    }

    protected void checkRequest(ParsedRequest request, String methodName, boolean isBucketAcl) throws S3Exception {
        S3_DRIVER.isBucketExists(request.getS3BucketPath());
        boolean aclCheckResult = S3_DRIVER.checkAclPermission(isBucketAcl, request.getS3ObjectPathUnsafe(),
                methodName, request.getS3User());
        boolean policyCheckResult = S3_DRIVER.checkBucketPolicy(request.getS3BucketPath(),
                request.getS3ObjectPathUnsafe().getKeyUnsafe(), methodName, request.getS3User());
        if (!(aclCheckResult && policyCheckResult)) {
            throw S3Exception.ACCESS_DENIED()
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    private void wrapProcess(Logger log, ChannelHandlerContext ctx, FullHttpRequest request, Process process) {
        try {
            process.process();
        } catch (S3Exception s3Exception) {
            sendError(ctx, request, s3Exception);
            log.error(s3Exception.getMessage(), s3Exception);
        } catch (Exception exception) {
            S3Exception s3Exception = S3Exception.INTERNAL_ERROR(exception.getMessage())
                    .setMessage(exception.getMessage())
                    .setResource("1")
                    .setRequestId("1");
            sendError(ctx, request, s3Exception);
            log.error(exception.getMessage(), exception);
        }
    }

    public void sendResponse(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponseStatus httpResponseStatus,
                             Consumer<FullHttpResponse> headersSetter, String content) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, httpResponseStatus, Unpooled.copiedBuffer(content, CharsetUtil.UTF_8));
        headersSetter.accept(response);
        sendAndCleanupConnection(ctx, response, request);
    }

    public void sendResponse(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponseStatus httpResponseStatus,
                             Consumer<FullHttpResponse> headersSetter, byte[] content) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, httpResponseStatus, Unpooled.copiedBuffer(content));
        headersSetter.accept(response);
        sendAndCleanupConnection(ctx, response, request);
    }

    public static void sendError(ChannelHandlerContext ctx, FullHttpRequest request, S3Exception s3Exception) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, s3Exception.getStatus(), Unpooled.copiedBuffer(s3Exception.buildXmlText(), CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/xml");
        sendAndCleanupConnection(ctx, response, request);
    }

    protected void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, FullHttpRequest request) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

        sendAndCleanupConnection(ctx, response, request);
    }

    protected void sendResponseWithoutContent(ChannelHandlerContext ctx, HttpResponseStatus status,
                                              FullHttpRequest request, Map<String, Object> headers) {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status);
        headers.forEach((header, value) -> response.headers().set(header, value));
        sendAndCleanupConnection(ctx, response, request);
    }

    protected static void sendAndCleanupConnection(ChannelHandlerContext ctx, FullHttpResponse response,
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

    public void process(ChannelHandlerContext context, FullHttpRequest request, ParsedRequest parsedRequest,
                        Object... arguments) throws Exception {
        if (!request.decoderResult().isSuccess()) {
            sendError(context, BAD_REQUEST, request);
            return;
        }
        wrapProcess(getLogger(), context, request, () -> processInner(context, request, parsedRequest, arguments));
    }

    protected abstract void processInner(ChannelHandlerContext context, FullHttpRequest request,
                                         ParsedRequest parsedRequest, Object... arguments) throws Exception;
    protected abstract Logger getLogger();
}
