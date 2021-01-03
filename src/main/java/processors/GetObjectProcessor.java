package processors;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class GetObjectProcessor extends Processor {

    public GetObjectProcessor(String baseBucketPath) {
        super(baseBucketPath);
    }

    @Override
    public boolean isThisProcessor(FullHttpRequest request) {
        if (!request.method().equals(HttpMethod.GET)) {
            return false;
        }
        return true;
    }

    @Override
    public void processInner(ChannelHandlerContext context, FullHttpRequest request)
            throws Exception {

        Optional<String> optionalPath = buildPath(context, request);
        if (optionalPath.isEmpty()) {
            sendError(context, FORBIDDEN, request);
            return;
        }
        final String path = optionalPath.get();
        System.out.println(path);

        final boolean keepAlive = HttpUtil.isKeepAlive(request);
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

}
