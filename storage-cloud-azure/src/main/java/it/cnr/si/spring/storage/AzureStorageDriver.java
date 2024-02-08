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

import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.blob.*;
import it.cnr.si.spring.storage.config.AzureStorageConfigurationProperties;
import it.cnr.si.spring.storage.config.StoragePropertyNames;
import it.cnr.si.util.MetadataEncodingUtils;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.http.auth.UsernamePasswordCredentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.html.Option;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by marco.spasiano on 06/07/17.
 */
public class AzureStorageDriver implements StorageDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureStorageDriver.class);

    private CloudBlobContainer cloudBlobContainer;
    private AzureStorageConfigurationProperties azureStorageConfigurationProperties;

    public AzureStorageDriver(CloudBlobContainer cloudBlobContainer, AzureStorageConfigurationProperties azureStorageConfigurationProperties) {
        this.cloudBlobContainer = cloudBlobContainer;
        this.azureStorageConfigurationProperties = azureStorageConfigurationProperties;
    }

    private HashMap<String, String> putUserMetadata(Map<String, Object> metadata) {
        return metadata
                .entrySet()
                .stream()
                .collect(HashMap::new, (m, entry) -> {
                    Optional.ofNullable(entry.getValue())
                            .ifPresent(entryValue -> {
                                String b64EncodedKey = MetadataEncodingUtils.encodeKey(entry.getKey());
                                if (entry.getKey().equals(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value())) {
                                    String base64EncodedValues = MetadataEncodingUtils
                                            .encodeValues((List<String>) entryValue);
                                    m.put(b64EncodedKey, base64EncodedValues);
                                } else {
                                    String base64EncodedValue = String.valueOf(entry.getValue());
                                    m.put(b64EncodedKey, MetadataEncodingUtils.encodeValue(base64EncodedValue));
                                }
                            });
                }, HashMap::putAll);
    }

    private Map<String, Object> getUserMetadata(CloudBlob blockBlobReference) {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put(StoragePropertyNames.CONTENT_STREAM_LENGTH.value(), BigInteger.valueOf(blockBlobReference.getProperties().getLength()));
        result.put(StoragePropertyNames.CONTENT_STREAM_MIME_TYPE.value(), blockBlobReference.getProperties().getContentType());
        result.put(StoragePropertyNames.ALFCMIS_NODEREF.value(), blockBlobReference.getName());
        result.put(StoragePropertyNames.ID.value(), blockBlobReference.getName());
        result.put(StoragePropertyNames.LAST_MODIFIED.value(), DateUtils.toCalendar(blockBlobReference.getProperties().getLastModified()));
        result.put(StoragePropertyNames.NAME.value(),
                Optional.ofNullable(blockBlobReference.getName().lastIndexOf(SUFFIX))
                        .filter(index -> index > -1)
                        .map(index -> blockBlobReference.getName().substring(index + 1))
                        .orElse(blockBlobReference.getName())
        );
        result.put(StoragePropertyNames.BASE_TYPE_ID.value(),
                Optional.of(blockBlobReference.getProperties().getLength())
                        .filter(aLong -> aLong > 0)
                        .map(aLong -> StoragePropertyNames.CMIS_DOCUMENT.value())
                        .orElse(StoragePropertyNames.CMIS_FOLDER.value()));

        blockBlobReference.getMetadata().forEach((b64EncodedKey, b64EncodedValue) -> {
            String key = MetadataEncodingUtils.decodeKey(b64EncodedKey);
            if (key.equals(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value())) {
                result.put(key, MetadataEncodingUtils.decodeValues(b64EncodedValue));
            } else {
                result.put(key, MetadataEncodingUtils.decodeValue(b64EncodedValue));
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
                    try {
                        final CloudAppendBlob blockBlobReference = cloudBlobContainer
                                .getAppendBlobReference(key);
                        //blockBlobReference.setMetadata(putUserMetadata(metadata));
                        //blockBlobReference.createOrReplace(); //TODO non crea un folder ma un file vuoto
                        //blockBlobReference.uploadMetadata();
                        HashMap<String, String> map = cloudBlobContainer
                                .getBlockBlobReference(key)
                                .getMetadata();

                        return new StorageObject(key, key, map);
                    } catch (URISyntaxException | com.microsoft.azure.storage.StorageException e) {
                        throw new StorageException(StorageException.Type.GENERIC, e);
                    }
                }).orElse(new StorageObject(key, key, Collections.emptyMap()));
    }

    private String cleanupContentType(String contentType) {
        try {
            return Optional.ofNullable(contentType)
                    .map(s -> {
                        final int i = s.indexOf(";");
                        if (i != -1)
                            return s.substring(0, i);
                        return s;
                    })
                    .orElse("application/octet-stream");
        }catch( Exception e ) {
            LOGGER.error(" cleanupContentType",e);
            return "application/octet-stream";
        }
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
        try {
            CloudBlockBlob blockBlobReference = cloudBlobContainer
                    .getBlockBlobReference(key);

            blockBlobReference
                    .upload(inputStream, -1);
            blockBlobReference.getProperties().setContentType(cleanupContentType( contentType));
            blockBlobReference.uploadProperties();
            blockBlobReference.setMetadata(putUserMetadata(metadataProperties));
            blockBlobReference.uploadMetadata();

            return new StorageObject(key, key, getUserMetadata(blockBlobReference));

        } catch (URISyntaxException | IOException | com.microsoft.azure.storage.StorageException e) {
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }

    private CloudBlobDirectory getClouBlobDirectory(StorageObject storageObject) throws com.microsoft.azure.storage.StorageException {
        try {
            Optional<CloudBlobDirectory> storageDirectory = cloudBlobContainer.listBlobsSegmented(storageObject.getKey())
                    .getResults()
                    .stream()
                    .filter(CloudBlobDirectory.class::isInstance)
                    .filter(c -> (getName((CloudBlobDirectory) c).equals(storageObject.getPath())))
                    .findFirst()
                    .map(CloudBlobDirectory.class::cast);
            if (storageDirectory.isPresent())
                return storageDirectory.get();
        } catch (com.microsoft.azure.storage.StorageException e) {
            if (e instanceof com.microsoft.azure.storage.StorageException) {
                if (((com.microsoft.azure.storage.StorageException) e).getHttpStatusCode() == 404) {
                    LOGGER.debug("Directory " + storageObject.getKey() + " does not exist", e);
                    throw e;
                }
            }
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
        return null;
    }

    private boolean isDirectory(StorageObject storageObject) {
        CloudBlobDirectory d = null;
        try {
            d = getClouBlobDirectory(storageObject);
        } catch (com.microsoft.azure.storage.StorageException e) {
            if (((com.microsoft.azure.storage.StorageException) e).getHttpStatusCode() == 404) {
                return false;
            }
        }
        if (d == null)
            return Boolean.FALSE;
        return Boolean.TRUE;

    }


    @Override
    public void updateProperties(StorageObject storageObject, Map<String, Object> metadataProperties) {

        CloudBlob blockBlobReference;
        try {
            /*I metedata sulle directory non sono supportati da Azure*/
            if (isDirectory(storageObject)) {
                CloudBlobDirectory storageDirectory = getClouBlobDirectory(storageObject);
                Optional.ofNullable(storageObject)
                        .map(so -> {
                            Optional.ofNullable(metadataProperties.get(StoragePropertyNames.NAME.value()))
                                    .ifPresent(name -> {

                                         try {
                                            String newDir = Optional.of(storageDirectory.getParent())
                                                    .filter(s->s!=null)
                                                    .map( s->getName(s))
                                                    .orElse("").concat(Optional.ofNullable((String)name)
                                                            .filter(s -> !s.equals(SUFFIX) && (!s.startsWith(SUFFIX)))
                                                            .map(s -> SUFFIX.concat(s))
                                                            .orElse((String)name));
                                             renameDirectory(storageObject, getObjectByPath(newDir, true));
                                             LOGGER.debug("folder Dest:" + getObjectByPath(newDir, true).getPath());

                                        } catch (URISyntaxException |com.microsoft.azure.storage.StorageException  e) {
                                           throw new StorageException(StorageException.Type.GENERIC,"errore Update Proprerties Rename Folder",e);
                                        }
                                    });
                            return storageObject.getKey();
                        });
                return;
            }
            blockBlobReference = cloudBlobContainer
                    .getBlobReferenceFromServer(storageObject.getKey());
            if (blockBlobReference.exists()) {
                HashMap<String, String> objectMetadataProperties = putUserMetadata(metadataProperties);
                HashMap<String, String> metadataPropertiesToSet = new HashMap<>(Optional.ofNullable(blockBlobReference.getMetadata()).orElse(new HashMap<String, String>()));

                objectMetadataProperties.forEach(
                        (key, value) -> metadataPropertiesToSet.merge(key, value, (v1, v2) -> v2));

                blockBlobReference.setMetadata(metadataPropertiesToSet);

                blockBlobReference.uploadMetadata();
                Optional.ofNullable(metadataProperties.get(StoragePropertyNames.NAME.value()))
                        .map(String.class::cast)
                        .filter(s -> !s.equals(blockBlobReference.getName()))
                        .ifPresent(s -> {
                            //TODO
                        });
            }
        } catch (URISyntaxException | com.microsoft.azure.storage.StorageException e) {
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }

    @Override
    public StorageObject updateStream(String key, InputStream inputStream, String contentType) {
        CloudBlob snapshot = null;
        try {
            CloudBlob blockBlobReference = cloudBlobContainer
                    .getBlobReferenceFromServer(key);

            HashMap<String, String> metadataSource = blockBlobReference.getMetadata();
            blockBlobReference
                    .upload(inputStream, -1);
            return new StorageObject(key, key, getUserMetadata(blockBlobReference));

        } catch (URISyntaxException | IOException | com.microsoft.azure.storage.StorageException e) {
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }

    @Override
    public InputStream getInputStream(String key) {
        try {
            return cloudBlobContainer
                    .getBlobReferenceFromServer(Optional.ofNullable(key)
                            .map(s -> {
                                if (s.indexOf(SUFFIX) == 0)
                                    return s.substring(1);
                                else
                                    return s;
                            }).orElseThrow(() -> new StorageException(StorageException.Type.NOT_FOUND, "Key is null")))
                    .openInputStream();
        } catch (URISyntaxException | com.microsoft.azure.storage.StorageException e) {
            throw new StorageException(StorageException.Type.GENERIC, e);
        }

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
        try {
            CloudBlob blobReference = cloudBlobContainer.getBlobReferenceFromServer(key);
            blobReference.delete(DeleteSnapshotsOption.INCLUDE_SNAPSHOTS, null /* accessCondition */, null /* options */, null /* opContext */);
        } catch (URISyntaxException | com.microsoft.azure.storage.StorageException e) {
            if (e instanceof com.microsoft.azure.storage.StorageException) {
                if (((com.microsoft.azure.storage.StorageException) e).getHttpStatusCode() == 404) {
                    LOGGER.debug("item " + key + " does not exist", e);
                    return false;
                }
            }
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
        return true;
    }

    private String getName(CloudBlobDirectory cloudBlobDirectory) {
        Optional<String> nameOpt = Optional.ofNullable(cloudBlobDirectory).map(dir -> dir.getPrefix()).
                filter(s -> (!s.isEmpty())).map(s -> s.substring(0, s.length() - 1));
        if (nameOpt.isPresent())
            return nameOpt.get();

        return null;
    }

    @Override
    public StorageObject getObject(String key) {
        key = Optional.ofNullable(key)
                .filter(s -> !s.equals(SUFFIX))
                .map(s -> (s.startsWith(SUFFIX) ? s.substring(1) : s))
                .map(s -> (s.endsWith(SUFFIX) ? s.substring(0, s.length() - 1) : s))
                .orElse(key);

        try {
            StorageObject checkDir = new StorageObject(key, key, Collections.emptyMap());
            if (isDirectory(checkDir)) {
                CloudBlobDirectory cloudBlobDirectory = getClouBlobDirectory(checkDir);
                return new StorageObject(
                        getName(cloudBlobDirectory),
                        getName(cloudBlobDirectory),
                        getUserMetadata(cloudBlobDirectory));
            }
            CloudBlob blobReference = cloudBlobContainer
                    .getBlobReferenceFromServer(key);
            return new StorageObject(key, key, getUserMetadata(blobReference));
        } catch (URISyntaxException | com.microsoft.azure.storage.StorageException e) {
            if (e instanceof com.microsoft.azure.storage.StorageException) {
                if (((com.microsoft.azure.storage.StorageException) e).getHttpStatusCode() == 404) {
                    LOGGER.debug("item " + key + " does not exist", e);
                    return null;
                }
            }
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }

    @Override
    public StorageObject getObject(String id, UsernamePasswordCredentials customCredentials) {
        return getObject(id);
    }

    @Override
    public StorageObject getObjectByPath(String path, boolean isFolder) {
        return Optional.ofNullable(getObject(path))
                .orElseGet(() -> {
                    if (isFolder) {
                        String key = Optional.ofNullable(path)
                                .filter(s -> !s.equals(SUFFIX) && s.startsWith(SUFFIX))
                                .map(s -> s.substring(1))
                                .orElse(path);
                        return new StorageObject(key, key, Collections.emptyMap());
                    } else {
                        return null;
                    }
                });
    }

    @Override
    public List<StorageObject> getChildren(String key, int depth) {
        return getChildren(key);
    }

    private List<StorageObject> getStorageObjectsFiles(List<ListBlobItem> l) {
        if ( Optional.ofNullable(l).isPresent()) {
            return l.stream()
                    .filter(CloudBlockBlob.class::isInstance)
                    .map(CloudBlockBlob.class::cast)
                    .map(cloudBlockBlob -> {
                        try {
                            return new StorageObject(
                                    cloudBlockBlob.getName(),
                                    cloudBlockBlob.getName(),
                                    getUserMetadata(cloudBlobContainer.getBlobReferenceFromServer(cloudBlockBlob.getName())));
                        } catch (URISyntaxException | com.microsoft.azure.storage.StorageException e) {
                            throw new StorageException(StorageException.Type.GENERIC, e);
                        }
                    })
                    .collect(Collectors.toList());
        }
        return new ArrayList<StorageObject>();
    }
    private Map<String, Object> getUserMetadata(CloudBlobDirectory cloudBlobDirectory) {
        Map<String, Object> result = new HashMap<String, Object>();

        result.put(StoragePropertyNames.BASE_TYPE_ID.value(),
                        StoragePropertyNames.CMIS_FOLDER.value());

        result.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(),
                Collections.emptyList());

        result.put(StoragePropertyNames.NAME.value(),getDirectoryName( cloudBlobDirectory));
        return result;
    }
    private List<StorageObject> getStorageObjectsDirectories(List<ListBlobItem> listBlobItem) {
        if ( Optional.ofNullable(listBlobItem).isPresent()) {
            return listBlobItem.stream()
                    .filter(CloudBlobDirectory.class::isInstance)
                    .map(CloudBlobDirectory.class::cast)
                    .map(cloudBlobDirectory -> {
                            return new StorageObject(
                                    sanitazeDirectoryPath(cloudBlobDirectory.getPrefix()),
                                    sanitazeDirectoryPath(cloudBlobDirectory.getPrefix()),
                                    getUserMetadata(cloudBlobDirectory));
                    })
                    .collect(Collectors.toList());
        }
        return new ArrayList<StorageObject>();
    }
    private List<StorageObject>getChildren(List<ListBlobItem> listBlobItem){
        List<StorageObject> childrens= getStorageObjectsFiles(listBlobItem );
        childrens.addAll( getStorageObjectsDirectories(listBlobItem));
        return childrens;
    }
    @Override
    public List<StorageObject> getChildren(String key) {

            key = Optional.ofNullable(key)
                    .filter(s -> !s.equals(SUFFIX) && s.startsWith(SUFFIX))
                    .map(s -> s.substring(1))
                    .orElse(key).concat(SUFFIX);
                try {
                   return getChildren(cloudBlobContainer.listBlobsSegmented(key).getResults());
                } catch (com.microsoft.azure.storage.StorageException e) {
                    throw new StorageException(StorageException.Type.GENERIC, e);
                }

        }


/*
            return cloudBlobContainer.listBlobsSegmented(directory.getPrefix()).getResults()
                    .stream()
                    .filter(CloudBlockBlob.class::isInstance)
                    .map(CloudBlockBlob.class::cast)
                    .map(cloudBlockBlob -> {
                        try {
                            return new StorageObject(
                                    cloudBlockBlob.getName(),
                                    cloudBlockBlob.getName(),
                                    getUserMetadata(cloudBlobContainer.getBlobReferenceFromServer(cloudBlockBlob.getName())));
                        } catch (URISyntaxException | com.microsoft.azure.storage.StorageException e) {
                            throw new StorageException(StorageException.Type.GENERIC, e);
                        }
                    })
                    .collect(Collectors.toList());

        } catch (URISyntaxException | com.microsoft.azure.storage.StorageException e) {
         throw new StorageException(StorageException.Type.GENERIC, e);
        }


    }
*/
    @Override
    public List<StorageObject> search(String query) {
        LOGGER.warn("AZURE -> Not yet implemented");
        return Collections.emptyList();
    }

    @Override
    public String signDocuments(String json, String url) {
        LOGGER.warn("AZURE -> Not yet implemented -> signDocuments");
        return null;
    }


    private void deleteDirectory(StorageObject directory) {
        if (directory == null || directory.getPath().isEmpty())
            return;
        if (isDirectory(directory)) {
            try {
                CloudBlobDirectory sourcedir = getClouBlobDirectory(directory);
                getSubDirectory(sourcedir).forEach(s -> {
                    LOGGER.debug("sub Directory:" + s.getPrefix());
                    StorageObject sourceSub = getObjectByPath(s.getPrefix(), true);
                    deleteDirectory(sourceSub);
                });
                //copia i file
                List<StorageObject> l = getChildren(directory.getKey());
                StorageObject currentSo;
                Optional.ofNullable(l).map(List::stream)
                        .ifPresent(ob -> ob.forEach(so -> {
                            try {
                                delete(so.getKey());
                                LOGGER.debug("Source Key:" + so.getKey() + " dir:" + so.getKey());
                            } catch (StorageException e) {
                                throw new StorageException(StorageException.Type.GENERIC, "Delete Directory errore in copia/delete file" + so.getKey());
                            }
                        }));

            } catch (StorageException | com.microsoft.azure.storage.StorageException e) {
                throw new StorageException(StorageException.Type.GENERIC, "Delete Directory errore cancellazione file", e);
            }
        }
    }

    private List<CloudBlobDirectory> getSubDirectory(CloudBlobDirectory directory) {
        try {
            return StreamSupport.stream(directory.listBlobs().spliterator(), false)
                    .filter(CloudBlobDirectory.class::isInstance)
                    .map(sc -> (CloudBlobDirectory) sc)
                    .collect(Collectors.toList());
        } catch (com.microsoft.azure.storage.StorageException | URISyntaxException e) {
            LOGGER.error("getSubDirectory:" + directory.getPrefix(), e);
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }

    private String sanitazeDirectoryPath(String path) {
        return Optional.ofNullable(path)
                .filter(s -> !s.equals(SUFFIX))
                .map(s -> (s.startsWith(SUFFIX) ? s.substring(1) : s))
                .map(s -> (s.endsWith(SUFFIX) ? s.substring(0, s.length() - 1) : s))
                .orElse(path);
    }

    private String getDirectoryName(CloudBlobDirectory directory) {
        if (directory == null)
            return null;

    String name = sanitazeDirectoryPath(directory.getPrefix());

            return Optional.ofNullable(name.lastIndexOf(SUFFIX))
                    .filter(index -> index > -1)
                    .map(index -> name.substring(index + 1))
                    .orElse(name);


    }

    private boolean isValidPathRenameDirectory(StorageObject source, StorageObject dest) {
        if (source == null || source.getPath().isEmpty())
            return false;
        if (dest == null || dest.getPath().isEmpty())
            return false;

        String pathParentSource =
                Optional.ofNullable(sanitazeDirectoryPath(source.getPath()).lastIndexOf(SUFFIX))
                        .filter(index -> index > -1)
                        .map(index -> sanitazeDirectoryPath(source.getPath()).substring(0, index - 1))
                        .orElse("");

        String pathParentDest =
                Optional.ofNullable(sanitazeDirectoryPath(dest.getPath()).lastIndexOf(SUFFIX))
                        .filter(index -> index > -1)
                        .map(index -> sanitazeDirectoryPath(dest.getPath()).substring(0, index - 1))
                        .orElse("");


        return pathParentSource.equals(pathParentDest);
    }

    private void renameDirectory(StorageObject source, StorageObject target) {
        if (source == null || source.getPath().isEmpty())
            throw new StorageException(StorageException.Type.GENERIC, "RenameDirectory Source Directory is null" + source.getKey());
        if (target == null || target.getPath().isEmpty())
            throw new StorageException(StorageException.Type.GENERIC, "RenameDirectory Dest Directory is null" + target.getKey());
        if (!isValidPathRenameDirectory(source, target))
            throw new StorageException(StorageException.Type.GENERIC, "Rename Directory->The parent Directory for Dest and Source is different:" + target.getPath() + "," + source.getPath());
        if (source.getPath().equalsIgnoreCase(target.getPath()))
            return;
        copyDirectory(source, target);
        deleteDirectory(source);


    }

    private void copyDirectory(StorageObject source, StorageObject target) {
        if (source.getPath().equalsIgnoreCase(target.getPath()))
            return;

        if (isDirectory(source)) {
            try {
                CloudBlobDirectory sourcedir = getClouBlobDirectory(source);
                getSubDirectory(sourcedir).forEach(s -> {
                    LOGGER.debug(s.getPrefix());
                    StorageObject targetSub = getObjectByPath(target.getPath().concat(SUFFIX).concat(getDirectoryName((CloudBlobDirectory) s)), true);
                    StorageObject sourceSub = getObjectByPath(s.getPrefix(), true);
                    LOGGER.debug(targetSub.getPath(), targetSub);
                    copyDirectory(sourceSub, targetSub);

                });
                //copy files
                List<StorageObject> l = getChildren(source.getKey());
                StorageObject currentSo;
                Optional.ofNullable(l).map(List::stream)
                        .ifPresent(ob -> ob.forEach(so -> {
                            try {
                                copyNode(so, target);
                                LOGGER.debug("Source Key:" + so.getKey() + " dir:" + target.getPath());
                            } catch (StorageException e) {
                                throw new StorageException(StorageException.Type.GENERIC, "Rename Directory  errore in copia/delete file" + so.getKey());
                            }
                        }));

            } catch (StorageException | com.microsoft.azure.storage.StorageException e) {
                deleteDirectory(target);
                throw new StorageException(StorageException.Type.GENERIC, "Rename Directory errore in copia/delete file");
            }
        }


    }

    @Override
    public void copyNode(StorageObject source, StorageObject target) {

        try {

            CloudBlob blobReference = cloudBlobContainer
                    .getBlockBlobReference(source.getKey());
            EnumSet<BlobListingDetails> listingDetails = EnumSet.of(BlobListingDetails.SNAPSHOTS);

            CloudBlobDirectory folder = cloudBlobContainer.getDirectoryReference(target.getPath());
            CloudBlockBlob newBlob = folder.getBlockBlobReference(source.getPropertyValue(StoragePropertyNames.NAME.value()));
            newBlob.startCopy(blobReference.getUri());
        } catch (URISyntaxException | com.microsoft.azure.storage.StorageException e) {
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }

    @Override
    public void managePermission(StorageObject storageObject, Map<String, ACLType> permission, boolean remove) {
        LOGGER.warn("AZURE -> Not yet implemented");
    }

    @Override
    public void setInheritedPermission(StorageObject storageObject, Boolean inherited) {
        LOGGER.warn("AZURE -> Not yet implemented");
    }

    @Override
    public List<StorageObject> getRelationship(String key, String relationshipName, boolean fromTarget) {
        LOGGER.warn("AZURE -> Not yet implemented");
        return Collections.emptyList();
    }

    @Override
    public void createRelationship(String source, String target, String relationshipName) {
        LOGGER.warn("AZURE -> Not yet implemented");
    }

    @Override
    public StoreType getStoreType() {
        return StoreType.AZURE;
    }

    @Override
    public void init() {
        LOGGER.debug("init {}...", AzureStorageDriver.class.getSimpleName());
    }


}
