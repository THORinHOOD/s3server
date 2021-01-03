package processors;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.multipart.*;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Optional;

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
                Optional<String> optionalPath = buildPath(context, request);
                if (optionalPath.isEmpty()) {
                    sendError(context, FORBIDDEN, request);
                    return;
                }
                final String path = optionalPath.get();
                System.out.println(path);

                File file = new File(path);
                if (file.exists()) {
                    sendError(context, BAD_REQUEST, request);
                    return;
                }
                if (file.createNewFile()) {
                    FileOutputStream outputStream = new FileOutputStream(file);
                    outputStream.write(fileUpload.get());
                    outputStream.close();
                } else {
                    sendError(context, INTERNAL_SERVER_ERROR, request);
                    return;
                }
                //TODO RESPONSE
            }
        }
    }

}
