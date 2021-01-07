package processors;

import data.S3Util;
import exceptions.S3Exception;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import utils.DateTimeUtil;
import utils.XmlUtil;

import java.io.File;
import java.util.Map;
import java.util.Optional;

public class CreateBucketProcessor extends Processor {

    public CreateBucketProcessor(String basePath) {
        super(basePath);
    }

    @Override
    public ProcessorPreArguments isThisProcessor(FullHttpRequest request) {
        if (!request.method().equals(HttpMethod.PUT)) {
            return new ProcessorPreArguments(false);
        }
        Optional<Document> content = XmlUtil.parseXmlFromByteBuf(request.content());
        if (content.isEmpty()) {
            return new ProcessorPreArguments(false);
        }
        NodeList nodeList = content.get().getChildNodes();
        if (nodeList.getLength() != 1 || !nodeList.item(0).getNodeName().equals("CreateBucketConfiguration")) {
            return new ProcessorPreArguments(false);
        }
        return new ProcessorPreArguments(true, content.get());
    }

    @Override
    protected void processInner(ChannelHandlerContext context, FullHttpRequest request, Object[] arguments)
            throws Exception {
        String bucket = extractBucket(request);
        try {
            S3Util.createBucket(bucket, BASE_PATH);
            sendResponseWithoutContent(context, HttpResponseStatus.OK, request, Map.of(
                "Date", DateTimeUtil.currentDateTime(),
                "Location", File.separatorChar + bucket
            ));
        } catch (S3Exception exception) {
            sendError(context, request, exception);
            exception.printStackTrace();
        }
    }
}
