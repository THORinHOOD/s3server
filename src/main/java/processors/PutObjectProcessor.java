package processors;

import data.S3Object;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.multipart.*;
import utils.DateTimeUtil;

import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class PutObjectProcessor extends Processor {

    private HttpPostRequestDecoder httpDecoder;
    private final HttpDataFactory factory = new DefaultHttpDataFactory(true);

    public PutObjectProcessor(String basePath, HttpPostRequestDecoder httpDecoder) {
        super(basePath);
        this.httpDecoder = httpDecoder;
    }

    @Override
    public boolean isThisProcessor(FullHttpRequest request) {
        if (!request.method().equals(HttpMethod.PUT)) {
            return false;
        }
        return true;
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request)
            throws Exception {
        httpDecoder = new HttpPostRequestDecoder(factory, request);

        List<InterfaceHttpData> datas = httpDecoder.getBodyHttpDatas();
        for (InterfaceHttpData data : datas) {
            if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.FileUpload) {
                DiskFileUpload fileUpload = (DiskFileUpload) data;

                String bucket = extractBucket(request);
                String key = extractKey(request);
                S3Object s3Object = S3Object.save(bucket, key, BASE_PATH, fileUpload.get());

                if (s3Object == null) {
                    sendError(context, INTERNAL_SERVER_ERROR, request);
                    return;
                }

                System.out.println(s3Object.getAbsolutePath());

                sendResponseWithoutContent(context, OK, request, Map.of(
                    "ETag", s3Object.getETag(),
                    "Last-Modified", s3Object.getLastModified(),
                    "Date", DateTimeUtil.currentDateTime()
                ));
                //TODO RESPONSE
            }
        }
    }

}
