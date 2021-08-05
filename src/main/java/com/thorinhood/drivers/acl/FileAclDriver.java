package com.thorinhood.drivers.acl;

import com.thorinhood.data.Owner;
import com.thorinhood.data.S3FileBucketPath;
import com.thorinhood.data.S3FileObjectPath;
import com.thorinhood.data.acl.AccessControlPolicy;
import com.thorinhood.data.acl.Grant;
import com.thorinhood.drivers.FileDriver;
import com.thorinhood.exceptions.S3Exception;
import com.thorinhood.utils.DateTimeUtil;
import com.thorinhood.utils.XmlUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

//Some usefull changes
public class FileAclDriver extends FileDriver implements AclDriver {

    public FileAclDriver(String baseFolderPath, String configFolderPath, String usersFolderPath) {
        super(baseFolderPath, configFolderPath, usersFolderPath);
    }

    @Override
    public String putObjectAcl(S3FileObjectPath s3FileObjectPath, AccessControlPolicy acl) throws S3Exception {
        String pathToObjectAclFile = s3FileObjectPath.getPathToObjectAclFile();
        String pathToMetadataFolder = s3FileObjectPath.getPathToObjectMetadataFolder();
        String xml = acl.buildXmlText();
        File metadataFolder = new File(pathToMetadataFolder);
        File metaFile = new File(pathToObjectAclFile);
        Path source = createPreparedTmpFile(metadataFolder.toPath(), metaFile.toPath(), xml.getBytes());
        commitFile(source, metaFile.toPath());
        return metaFile.exists() ? DateTimeUtil.parseDateTime(metaFile) : null;
    }

    @Override
    public AccessControlPolicy getObjectAcl(S3FileObjectPath s3FileObjectPath) throws S3Exception {
        return getAcl(s3FileObjectPath, s3FileObjectPath::getPathToObjectAclFile);
    }

    @Override
    public void putBucketAcl(S3FileBucketPath s3FileBucketPath, AccessControlPolicy acl) throws S3Exception {
        String xml = acl.buildXmlText();
        File metadataFolder = new File(s3FileBucketPath.getPathToBucketMetadataFolder());
        File metaFile = new File(s3FileBucketPath.getPathToBucketAclFile());
        Path source = createPreparedTmpFile(metadataFolder.toPath(), metaFile.toPath(), xml.getBytes());
        commitFile(source, metaFile.toPath());
    }

    @Override
    public AccessControlPolicy getBucketAcl(S3FileBucketPath s3FileBucketPath) throws S3Exception {
        return getAcl(s3FileBucketPath, s3FileBucketPath::getPathToBucketAclFile);
    }

    private AccessControlPolicy getAcl(S3FileBucketPath s3FileBucketPath, Supplier<String> pathGetter)
            throws S3Exception {
        String pathToAclFile = pathGetter.get();
        File file = new File(pathToAclFile);
        if (!file.exists()) {
            throw S3Exception.INTERNAL_ERROR("Can't find acl file : " + pathToAclFile);
        }
        byte[] bytes = null;
        try {
            bytes = new FileInputStream(file).readAllBytes();
        } catch (IOException exception) {
            throw S3Exception.INTERNAL_ERROR(exception);
        }
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
