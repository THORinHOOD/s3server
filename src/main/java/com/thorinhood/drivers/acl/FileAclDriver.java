package com.thorinhood.drivers.acl;

import com.thorinhood.acl.AccessControlPolicy;
import com.thorinhood.acl.Grant;
import com.thorinhood.acl.Owner;
import com.thorinhood.drivers.acl.AclDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileAclDriver implements AclDriver {

    @Override
    public boolean init() throws Exception {
        return true;
    }

    @Override
    public String putObjectAcl(String key, AccessControlPolicy acl) throws S3Exception {
        String pathToMetafile = getPathToObjectAclFile(key);
        return putAcl(pathToMetafile, acl);
    }

    @Override
    public AccessControlPolicy getObjectAcl(String key) throws S3Exception {
        String pathToMetafile = getPathToObjectAclFile(key);
        return getAcl(pathToMetafile);
    }

    @Override
    public void putBucketAcl(String basePath, String bucket, AccessControlPolicy acl) throws S3Exception {
        String pathToMetafile = getPathToBucketAclFile(basePath, bucket);
        putAcl(pathToMetafile, acl);
    }

    @Override
    public AccessControlPolicy getBucketAcl(String basePath, String bucket) throws S3Exception {
        String pathToMetafile = getPathToBucketAclFile(basePath, bucket);
        return getAcl(pathToMetafile);
    }

    private String putAcl(String pathToMetafile, AccessControlPolicy acl) {
        String xml = acl.buildXmlText();
        File metaFile = new File(pathToMetafile);
        String lastModified = null;
        if (metaFile.exists()) {
            lastModified = DateTimeUtil.parseDateTime(metaFile);
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

    private String getPathToBucketAclFile(String basePath, String bucket) {
        return basePath + File.separatorChar + bucket + ".acl";
    }

    private String getPathToObjectAclFile(String key) {
        return key.substring(0, key.lastIndexOf(".")) + ".acl";
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
