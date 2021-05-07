package com.thorinhood.drivers.acl;

import com.thorinhood.data.Owner;
import com.thorinhood.data.S3BucketPath;
import com.thorinhood.data.S3ObjectPath;
import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.data.acl.Grant;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.drivers.PreparedOperationFileCommit;
import com.thorinhood.drivers.PreparedOperationFileCommitWithResult;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class FileAclDriver extends FileDriver implements AclDriver {

    public FileAclDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
    }

    @Override
    public PreparedOperationFileCommitWithResult<String> putObjectAcl(S3ObjectPath s3ObjectPath, AccessControlPolicy acl)
            throws S3Exception {
        String pathToMetafile = getPathToObjectAclFile(s3ObjectPath, true);
        String pathToMetadataFolder = getPathToObjectMetadataFolder(s3ObjectPath, true);
        return putAcl(pathToMetadataFolder, pathToMetafile, acl);
    }

    @Override
    public AccessControlPolicy getObjectAcl(S3ObjectPath s3ObjectPath) throws S3Exception {
        String pathToMetafile = getPathToObjectAclFile(s3ObjectPath, true);
        return getAcl(pathToMetafile);
    }

    @Override
    public PreparedOperationFileCommit putBucketAcl(S3BucketPath s3BucketPath, AccessControlPolicy acl)
            throws S3Exception {
        String pathToMetafile = getPathToBucketAclFile(s3BucketPath, true);
        String pathToMetadataFolder = getPathToBucketMetadataFolder(s3BucketPath, true);
        return putAcl(pathToMetadataFolder, pathToMetafile, acl);
    }

    @Override
    public AccessControlPolicy getBucketAcl(S3BucketPath s3BucketPath) throws S3Exception {
        String pathToMetafile = getPathToBucketAclFile(s3BucketPath, true);
        return getAcl(pathToMetafile);
    }

    private PreparedOperationFileCommitWithResult<String> putAcl(String pathToMetadataFolder, String pathToMetafile,
                                                                 AccessControlPolicy acl) throws S3Exception {
        String xml = acl.buildXmlText();
        File metaFile = new File(pathToMetafile);
        String lastModified = metaFile.exists() ? DateTimeUtil.parseDateTime(metaFile) : null;
        Path source = createPreparedTmpFile(new File(pathToMetadataFolder).toPath(), metaFile.toPath(), xml.getBytes());
        return new PreparedOperationFileCommitWithResult<>(source, metaFile.toPath(),
                lastModified != null ? lastModified : DateTimeUtil.parseDateTime(new File(pathToMetafile)));
    }

    private AccessControlPolicy getAcl(String pathToMetaFile) throws S3Exception {
        File file = new File(pathToMetaFile);
        if (!file.exists()) {
            throw S3Exception.INTERNAL_ERROR("Can't find acl file : " + pathToMetaFile)
                    .setMessage("Can't find acl file : " + pathToMetaFile)
                    .setResource("1")
                    .setRequestId("1");
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

    private String getPathToBucketAclFile(S3BucketPath s3BucketPath, boolean safely) {
        return getPathToBucketMetadataFolder(s3BucketPath, safely) + File.separatorChar + s3BucketPath.getBucket() +
                ".acl";
    }

    private String getPathToObjectAclFile(S3ObjectPath s3ObjectPath, boolean safely) {
        return getPathToObjectMetadataFolder(s3ObjectPath, safely) + File.separatorChar + s3ObjectPath.getName() + ".acl";
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
