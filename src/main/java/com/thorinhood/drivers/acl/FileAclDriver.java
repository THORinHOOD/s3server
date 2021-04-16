package com.thorinhood.drivers.acl;

import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.data.acl.Grant;
import com.thorinhood.data.acl.Owner;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.drivers.FileMetadataSubDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileAclDriver extends FileMetadataSubDriver implements AclDriver {

    public FileAclDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
    }

    @Override
    public String putObjectAcl(String bucket, String key, AccessControlPolicy acl) throws S3Exception {
        String pathToMetafile = getPathToObjectAclFile(bucket, key);
        return putAcl(pathToMetafile, acl);
    }

    @Override
    public AccessControlPolicy getObjectAcl(String bucket, String key) throws S3Exception {
        String pathToMetafile = getPathToObjectAclFile(bucket, key);
        return getAcl(pathToMetafile);
    }

    @Override
    public void putBucketAcl(String bucket, AccessControlPolicy acl) throws S3Exception {
        String pathToMetafile = getPathToBucketAclFile(bucket);
        putAcl(pathToMetafile, acl);
    }

    @Override
    public AccessControlPolicy getBucketAcl(String bucket) throws S3Exception {
        String pathToMetafile = getPathToBucketAclFile(bucket);
        return getAcl(pathToMetafile);
    }

    private String putAcl(String pathToMetafile, AccessControlPolicy acl) throws S3Exception {
        S3Exception s3exception = S3Exception.INTERNAL_ERROR("Can't create acl bucket file")
                .setMessage("Can't create acl bucket file")
                .setResource("1")
                .setRequestId("1"); // TODO
        String xml = acl.buildXmlText();
        File metaFile = new File(pathToMetafile);
        String lastModified = null;
        if (metaFile.exists()) {
            lastModified = DateTimeUtil.parseDateTime(metaFile);
        } else {

            try {
                if (!metaFile.createNewFile()) {
                    throw s3exception;
                }
            } catch (IOException exception) {
                throw s3exception;
            }
        }
        try (FileOutputStream writer = new FileOutputStream(pathToMetafile)) {
            writer.write(xml.getBytes());
            writer.flush();
            return lastModified != null ? lastModified : DateTimeUtil.parseDateTime(new File(pathToMetafile)); // TODO
        } catch (IOException e) {
            throw S3Exception.INTERNAL_ERROR(e.getMessage())
                    .setMessage(e.getMessage())
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    private AccessControlPolicy getAcl(String pathToMetaFile) {
        File file = new File(pathToMetaFile);
        if (!file.exists()) {
            return null; // TODO
        }
        try {
            byte[] bytes = new FileInputStream(file).readAllBytes();
            Document document = XmlUtil.parseXmlFromBytes(bytes);
            return AccessControlPolicy.buildFromNode(document.getDocumentElement());
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception.getMessage())
                    .setMessage(exception.getMessage())
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    private String getPathToBucketAclFile(String bucket) {
        return getPathToBucketMetadataFolder(bucket) + File.separatorChar + bucket + ".acl";
    }

    private String getPathToObjectAclFile(String bucket, String key) {
        String fileName = extractFileName(key);
        return getPathToObjectMetadataFolder(bucket, key) + File.separatorChar + fileName + ".acl";
    }

    public AccessControlPolicy parseFromBytes(byte[] bytes) throws S3Exception {
        Document document = XmlUtil.parseXmlFromBytes(bytes);
        Element accessControlPolicyNode = (Element) document.getElementsByTagName("AccessControlPolicy").item(0);
        AccessControlPolicy.Builder aclBuilder = AccessControlPolicy.builder();
        aclBuilder.setXmlns(accessControlPolicyNode.getAttributes().getNamedItem("xmlns").getNodeValue());

        Owner owner = Owner.buildFromNode(accessControlPolicyNode.getElementsByTagName("Owner").item(0));
        aclBuilder.setOwner(owner);

        List<Grant> grantList = new ArrayList<>();
        Node grants = accessControlPolicyNode.getElementsByTagName("AccessControlList").item(0);
        for (int i = 0; i < grants.getChildNodes().getLength(); i++) {
            Grant grant = Grant.buildFromNode(grants.getChildNodes().item(i));
            grantList.add(grant);
        }
        aclBuilder.setAccessControlList(grantList);
        AccessControlPolicy acl = aclBuilder.build();
        return acl;
    }

    private String inputStreamToString(InputStream is) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

}
