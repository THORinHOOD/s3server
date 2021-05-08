package com.thorinhood.drivers.acl;

import com.thorinhood.data.Owner;
import com.thorinhood.data.S3BucketPath;
import com.thorinhood.data.S3ObjectPath;
import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.data.acl.Grant;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.drivers.lock.EntityLocker;
import com.thorinhood.drivers.lock.PreparedOperationFileWrite;
import com.thorinhood.drivers.lock.PreparedOperationFileWriteWithResult;
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

    public FileAclDriver(String baseFolderPath, String configFolderPath, String usersFolderPath,
                         EntityLocker entityLocker) {
        super(baseFolderPath, configFolderPath, usersFolderPath, entityLocker);
    }

    @Override
    public PreparedOperationFileWriteWithResult<String> putObjectAcl(S3ObjectPath s3ObjectPath, AccessControlPolicy acl)
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
    public PreparedOperationFileWrite putBucketAcl(S3BucketPath s3BucketPath, AccessControlPolicy acl)
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

    private PreparedOperationFileWriteWithResult<String> putAcl(String pathToMetadataFolder, String pathToMetafile,
                                                                AccessControlPolicy acl) throws S3Exception {
        String xml = acl.buildXmlText();
        File metaFile = new File(pathToMetafile);
        String lastModified = metaFile.exists() ? DateTimeUtil.parseDateTime(metaFile) : null;
        Path source = createPreparedTmpFile(new File(pathToMetadataFolder).toPath(), metaFile.toPath(), xml.getBytes());
        return new PreparedOperationFileWriteWithResult<>(source, metaFile.toPath(),
                lastModified != null ? lastModified : DateTimeUtil.parseDateTime(new File(pathToMetafile)),
                ENTITY_LOCKER);
    }

    private AccessControlPolicy getAcl(String pathToMetaFile) throws S3Exception {
        File file = new File(pathToMetaFile);
        if (!file.exists()) {
            throw S3Exception.INTERNAL_ERROR("Can't find acl file : " + pathToMetaFile)
                    .setMessage("Can't find acl file : " + pathToMetaFile)
                    .setResource("1")
                    .setRequestId("1");
        }
        byte[] bytes = ENTITY_LOCKER.read(file.getAbsolutePath(), () -> new FileInputStream(file).readAllBytes());
        Document document = XmlUtil.parseXmlFromBytes(bytes);
        return AccessControlPolicy.buildFromNode(document.getDocumentElement());
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
        return aclBuilder.build();
    }

}
