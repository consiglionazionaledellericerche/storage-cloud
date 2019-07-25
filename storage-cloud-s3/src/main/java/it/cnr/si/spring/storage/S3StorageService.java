/*
 * Copyright (C) 2019  Consiglio Nazionale delle Ricerche
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as
 *     published by the Free Software Foundation, either version 3 of the
 *     License, or (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package it.cnr.si.spring.storage;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import it.cnr.si.spring.storage.config.S3StorageConfigurationProperties;
import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by marco.spasiano on 06/07/17.
 */
@Service
public class S3StorageService implements StorageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageService.class);

    private AmazonS3 amazonS3;
    private S3StorageConfigurationProperties s3StorageConfigurationProperties;


    public S3StorageService(S3StorageConfigurationProperties s3StorageConfigurationProperties, AmazonS3 amazonS3) {
        this.s3StorageConfigurationProperties = s3StorageConfigurationProperties;
        this.amazonS3 = amazonS3;
    }

    private void setUserMetadata(ObjectMetadata objectMetadata, Map<String, Object> metadata) {

        Map<String, String> metadataKeys = s3StorageConfigurationProperties.getMetadataKeys();

        metadata
                .keySet()
                .stream()
                .forEach(key -> {
                    if (metadataKeys.containsKey(key)) {
                        if (metadataKeys.get(key) != null) {
                            if (key.equals(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value())) {
                                objectMetadata.addUserMetadata(metadataKeys.get(key),
                                        String.join(",", (List<String>) metadata.get(key)));
                            } else {
                                objectMetadata.addUserMetadata(metadataKeys.get(key), String.valueOf(metadata.get(key)));
                            }
                        }
                    }
                });
    }

    private Map<String, ?> getUserMetadata(String key, ObjectMetadata objectMetadata) {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put(StoragePropertyNames.CONTENT_STREAM_LENGTH.value(), BigInteger.valueOf(objectMetadata.getContentLength()));
        result.put(StoragePropertyNames.CONTENT_STREAM_MIME_TYPE.value(), objectMetadata.getContentType());
        result.put(StoragePropertyNames.ALFCMIS_NODEREF.value(), key);
        result.put(StoragePropertyNames.ID.value(), key);
        result.put(StoragePropertyNames.NAME.value(),
                Optional.ofNullable(key.lastIndexOf(SUFFIX))
                        .filter(index -> index > -1)
                        .map(index -> key.substring(index) + 1)
                        .orElse(key)
        );
        result.put(StoragePropertyNames.BASE_TYPE_ID.value(),
                Optional.of(objectMetadata.getContentLength())
                        .filter(aLong -> aLong > 0)
                        .map(aLong -> StoragePropertyNames.CMIS_DOCUMENT.value())
                        .orElse(StoragePropertyNames.CMIS_FOLDER.value()));
        s3StorageConfigurationProperties.getMetadataKeys().entrySet().stream()
                .forEach(entry -> {
                    final String userMetaDataOf = objectMetadata.getUserMetaDataOf(entry.getValue());
                    if (entry.getKey().equals(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value())) {
                        result.put(entry.getKey(), Optional.ofNullable(userMetaDataOf)
                                .map(s -> Arrays.asList(s.split(","))).orElse(Collections.emptyList()));
                    } else {
                        result.put(entry.getKey(), userMetaDataOf);
                    }
                });

        return result;
    }

    @Override
    public StorageObject createFolder(String path, String name, Map<String, Object> metadata) {
        final String key = Optional.ofNullable(path)
                .filter(s -> s.length() > 0)
                .filter(s -> !s.equals(SUFFIX))
                .map(s -> s.startsWith(SUFFIX) ? s.substring(1) : s)
                .map(s -> s.concat(SUFFIX).concat(name))
                .orElse(name);
        metadata.remove(StoragePropertyNames.NAME.value());
        Optional.ofNullable(metadata.get(StoragePropertyNames.OBJECT_TYPE_ID.value()))
                .filter(o -> o.equals(StoragePropertyNames.CMIS_FOLDER.value()))
                .ifPresent(o -> {
                    metadata.remove(StoragePropertyNames.OBJECT_TYPE_ID.value());
                });
        return Optional.ofNullable(metadata)
                .filter(stringObjectMap -> !stringObjectMap.isEmpty())
                .map(stringObjectMap -> {
                    ObjectMetadata objectMetadata = new ObjectMetadata();
                    setUserMetadata(objectMetadata, stringObjectMap);
                    objectMetadata.setContentLength(0);
                    InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
                    PutObjectResult putObjectResult = amazonS3.putObject(
                            s3StorageConfigurationProperties.getBucketName(),
                            key,
                            emptyContent,
                            objectMetadata);
                    return new StorageObject(key, key, putObjectResult.getMetadata().getUserMetadata());
                }).orElse(new StorageObject(key, key, Collections.emptyMap()));
    }

    @Override
    public StorageObject createDocument(InputStream inputStream, String contentType, Map<String, Object> metadataProperties,
                                        StorageObject parentObject, String path, boolean makeVersionable, Permission... permissions) {
        String parentPath = Optional.ofNullable(parentObject)
                .map(storageObject -> storageObject.getPath())
                .map(s -> {
                    if (s.indexOf(SUFFIX) == 0)
                        return s.substring(1);
                    else
                        return s;
                })
                .orElseGet(() -> {
                    if (path.indexOf(SUFFIX) == 0)
                        return path.substring(1);
                    else
                        return path;
                });
        String key = parentPath.concat(SUFFIX).concat((String) metadataProperties.get(StoragePropertyNames.NAME.value()));
        ObjectMetadata objectMetadata = new ObjectMetadata();
        setUserMetadata(objectMetadata, metadataProperties);
        try {
            objectMetadata.setContentLength(Long.valueOf(inputStream.available()).longValue());
            PutObjectResult putObjectResult = amazonS3.putObject(s3StorageConfigurationProperties.getBucketName(),
                    key, inputStream, objectMetadata);

            return new StorageObject(key, key, getUserMetadata(key, putObjectResult.getMetadata()));
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }

    @Override
    public void updateProperties(StorageObject storageObject, Map<String, Object> metadataProperties) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        setUserMetadata(objectMetadata, metadataProperties);
        S3Object s3Object = amazonS3.getObject(s3StorageConfigurationProperties.getBucketName(), storageObject.getKey());
        final PutObjectResult putObjectResult = amazonS3.putObject(s3StorageConfigurationProperties.getBucketName(), s3Object.getKey(), s3Object.getObjectContent(), objectMetadata);
        Optional.ofNullable(metadataProperties.get(StoragePropertyNames.NAME.value()))
                .map(String.class::cast)
                .filter(s -> !s.equals(s3Object.getKey().substring(s3Object.getKey().lastIndexOf(SUFFIX))))
                .ifPresent(s -> {
                    amazonS3.copyObject(s3StorageConfigurationProperties.getBucketName(),
                            s3Object.getKey(),
                            s3StorageConfigurationProperties.getBucketName(),
                            s3Object.getKey().substring(s3Object.getKey().lastIndexOf(SUFFIX) + 1).concat(s));
                    amazonS3.deleteObject(s3StorageConfigurationProperties.getBucketName(), s3Object.getKey());
                });
    }

    @Override
    public StorageObject updateStream(String key, InputStream inputStream, String contentType) {
        try {
            final byte[] bytes = IOUtils.toByteArray(inputStream);
            ObjectMetadata objectMetadata = amazonS3.getObject(s3StorageConfigurationProperties.getBucketName(), key).getObjectMetadata().clone();
            objectMetadata.setContentType(contentType);
            objectMetadata.setContentLength(Long.valueOf(bytes.length));
            PutObjectResult putObjectResult = amazonS3.putObject(s3StorageConfigurationProperties.getBucketName(),
                    key, new ByteArrayInputStream(bytes), objectMetadata);
            return new StorageObject(key, key, getUserMetadata(key, putObjectResult.getMetadata()));
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }

    @Override
    public InputStream getInputStream(String key) {
        return amazonS3.getObject(s3StorageConfigurationProperties.getBucketName(),
                Optional.ofNullable(key)
                        .map(s -> {
                            if (s.indexOf(SUFFIX) == 0)
                                return s.substring(1);
                            else
                                return s;
                        }).orElseThrow(() -> new StorageException(StorageException.Type.NOT_FOUND, "Key is null"))
        ).getObjectContent();
    }

    @Override
    public InputStream getInputStream(String key, String versionId) {
        return getInputStream(key);
    }

    @Override
    public InputStream getInputStream(String key, Boolean majorVersion) {
        return getInputStream(key);
    }

    @Override
    public Boolean delete(String key) {
        key = Optional.ofNullable(key)
                .filter(s -> !s.equals(SUFFIX) && s.startsWith(SUFFIX))
                .map(s -> s.substring(1))
                .orElse(key);
        boolean exists = amazonS3.doesObjectExist(s3StorageConfigurationProperties.getBucketName(), key);
        if (exists) {
            amazonS3.deleteObject(s3StorageConfigurationProperties.getBucketName(), key);
        } else {
            LOGGER.warn("item {} does not exist", key);
        }
        return exists;
    }

    @Override
    public StorageObject getObject(String key) {
        try {
            key = Optional.ofNullable(key)
                    .filter(s -> !s.equals(SUFFIX) && s.startsWith(SUFFIX))
                    .map(s -> s.substring(1))
                    .orElse(key);
            S3Object s3Object = amazonS3.getObject(s3StorageConfigurationProperties.getBucketName(), key);
            return new StorageObject(s3Object.getKey(), s3Object.getKey(), getUserMetadata(s3Object.getKey(), s3Object.getObjectMetadata()));
        } catch (AmazonS3Exception _ex) {
            if (_ex.getStatusCode() == HttpStatus.SC_NOT_FOUND)
                return null;
            throw new StorageException(StorageException.Type.GENERIC, _ex);
        }
    }

    @Override
    public StorageObject getObject(String id, UsernamePasswordCredentials customCredentials) {
        return getObject(id);
    }

    @Override
    public StorageObject getObjectByPath(String path, boolean isFolder) {
        return getObject(path);
    }

    @Override
    public List<StorageObject> getChildren(String key, int depth) {
        return getChildren(key);
    }

    @Override
    public List<StorageObject> getChildren(String key) {
        return amazonS3
                .listObjects(s3StorageConfigurationProperties.getBucketName(), key)
                .getObjectSummaries()
                .stream()
                .filter(s3ObjectSummary -> !s3ObjectSummary.getKey().equals(key))
                .map(s3Object -> getObject(s3Object.getKey()))
                .collect(Collectors.toList());
    }

    @Override
    public List<StorageObject> search(String query) {
        LOGGER.warn("S3 -> Not yet implemented");
        return Collections.emptyList();
    }

    @Override
    public String signDocuments(String json, String url) {
        LOGGER.warn("S3 -> Not yet implemented -> signDocuments");
        return null;
    }

    @Override
    public void copyNode(StorageObject source, StorageObject target) {
        String targetPath = Optional.ofNullable(target.getPath())
                .filter(s -> s.startsWith(SUFFIX))
                .map(s -> s.substring(1))
                .orElse(target.getPath())
                .concat(source.getKey().substring(source.getKey().lastIndexOf(SUFFIX)));
        amazonS3.copyObject(s3StorageConfigurationProperties.getBucketName(),
                source.getKey(),
                s3StorageConfigurationProperties.getBucketName(),
                targetPath);
    }

    @Override
    public void managePermission(StorageObject storageObject, Map<String, ACLType> permission, boolean remove) {
        LOGGER.warn("S3 -> Not yet implemented");
    }

    @Override
    public void setInheritedPermission(StorageObject storageObject, Boolean inherited) {
        LOGGER.warn("S3 -> Not yet implemented");
    }

    @Override
    public List<StorageObject> getRelationship(String key, String relationshipName, boolean fromTarget) {
        LOGGER.warn("S3 -> Not yet implemented");
        return Collections.emptyList();
    }

    @Override
    public void createRelationship(String source, String target, String relationshipName) {
        LOGGER.warn("S3 -> Not yet implemented");
    }

    @Override
    public StoreType getStoreType() {
        return StoreType.S3;
    }

    @Override
    public void init() {
        LOGGER.info("init {}...", S3StorageService.class.getSimpleName());
    }


}
