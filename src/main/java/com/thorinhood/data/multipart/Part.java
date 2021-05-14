package com.thorinhood.data.multipart;

import com.thorinhood.data.requests.S3ResponseErrorCodes;
import com.thorinhood.exceptions.S3Exception;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.w3c.dom.Node;

public class Part {

    private final String eTag;
    private final int partNumber;

    public static Part buildFromNode(Node node) throws S3Exception {
        String eTag = null;
        int partNumber = 0;
        for (int i = 0; i < node.getChildNodes().getLength(); i++) {
            Node child = node.getChildNodes().item(i);
            if (child.getNodeName().equals("ETag")) {
                eTag = child.getChildNodes().item(0).getNodeValue();
            } else if (child.getNodeName().equals("PartNumber")) {
                try {
                    partNumber = Integer.parseInt(child.getChildNodes().item(0).getNodeValue());
                } catch (Exception exception) {
                    throw S3Exception.builder("Part number must be an integer between 1 and 10000, " +
                            "inclusive")
                        .setStatus(HttpResponseStatus.BAD_REQUEST)
                        .setCode(S3ResponseErrorCodes.INVALID_ARGUMENT)
                        .setMessage("Part number must be an integer between 1 and 10000, inclusive")
                        .build();
                }
            }
        }

        if (eTag == null) {
            throw S3Exception.builder("Missed ETag value for one of the parts")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_REQUEST)
                    .setMessage("Missed ETag value for one of the parts")
                    .build();
        }
        if (partNumber < 1 || partNumber > 10000) {
            throw S3Exception.builder("Part number must be an integer between 1 and 10000, inclusive")
                    .setStatus(HttpResponseStatus.BAD_REQUEST)
                    .setCode(S3ResponseErrorCodes.INVALID_ARGUMENT)
                    .setMessage("Part number must be an integer between 1 and 10000, inclusive")
                    .build();
        }
        return new Part(eTag, partNumber);
    }

    private Part(String eTag, int partNumber) {
        this.eTag = eTag;
        this.partNumber = partNumber;
    }

    public String getETag() {
        return eTag;
    }

    public int getPartNumber() {
        return partNumber;
    }
}
