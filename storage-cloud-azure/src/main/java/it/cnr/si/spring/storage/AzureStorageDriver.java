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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.*;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;

import it.cnr.si.spring.storage.config.AzureStorageConfigurationProperties;
import it.cnr.si.spring.storage.config.StoragePropertyNames;
import it.cnr.si.util.MetadataEncodingUtils;


import org.apache.http.auth.UsernamePasswordCredentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by marco.spasiano on 06/07/17.
 */
public class AzureStorageDriver implements StorageDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureStorageDriver.class);

    private final BlobContainerClient blobContainerClient;
    private final AzureStorageConfigurationProperties azureStorageConfigurationProperties;

    public AzureStorageDriver(
            BlobContainerClient blobContainerClient,
            AzureStorageConfigurationProperties azureStorageConfigurationProperties) {

        this.blobContainerClient = blobContainerClient;
        this.azureStorageConfigurationProperties = azureStorageConfigurationProperties;
    }

    private HashMap<String, String> putUserMetadata(
            Map<String, Object> metadata) {

        HashMap<String, String> result = new HashMap<>();

        if (metadata == null) {
            return result;
        }

        metadata.forEach((key, value) -> {
            if (value == null) {
                return;
            }

            String encodedKey =
                    MetadataEncodingUtils.encodeKey(key);

            if (StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS
                    .value()
                    .equals(key)) {

                @SuppressWarnings("unchecked")
                List<String> values = (List<String>) value;

                result.put(
                        encodedKey,
                        MetadataEncodingUtils.encodeValues(values)
                );

            } else {
                result.put(
                        encodedKey,
                        MetadataEncodingUtils.encodeValue(
                                String.valueOf(value)
                        )
                );
            }
        });

        return result;
    }

    private Map<String, Object> getUserMetadata(BlobClient blobClient) {
        BlobProperties properties = blobClient.getProperties();

        Map<String, Object> result = new HashMap<>();

        result.put(
                StoragePropertyNames.CONTENT_STREAM_LENGTH.value(),
                BigInteger.valueOf(properties.getBlobSize())
        );

        result.put(
                StoragePropertyNames.CONTENT_STREAM_MIME_TYPE.value(),
                properties.getContentType()
        );

        result.put(
                StoragePropertyNames.ALFCMIS_NODEREF.value(),
                blobClient.getBlobName()
        );

        result.put(
                StoragePropertyNames.ID.value(),
                blobClient.getBlobName()
        );

        if (properties.getLastModified() != null) {
            Calendar lastModified = Calendar.getInstance();
            lastModified.setTime(Date.from(properties.getLastModified().toInstant()));

            result.put(
                    StoragePropertyNames.LAST_MODIFIED.value(),
                    lastModified
            );
        }

        result.put(
                StoragePropertyNames.NAME.value(),
                extractName(blobClient.getBlobName())
        );

        result.put(
                StoragePropertyNames.BASE_TYPE_ID.value(),
                StoragePropertyNames.CMIS_DOCUMENT.value()
        );

        properties.getMetadata().forEach((encodedKey, encodedValue) -> {
            String key = MetadataEncodingUtils.decodeKey(encodedKey);

            if (StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value().equals(key)) {
                result.put(key, MetadataEncodingUtils.decodeValues(encodedValue));
            } else {
                result.put(key, MetadataEncodingUtils.decodeValue(encodedValue));
            }
        });

        return result;
    }

    private String extractName(String path) {
        String normalized =
                removeTrailingSeparator(path);

        if (normalized == null || normalized.isEmpty()) {
            return normalized;
        }

        int separatorIndex =
                normalized.lastIndexOf(SUFFIX);

        if (separatorIndex < 0) {
            return normalized;
        }

        return normalized.substring(
                separatorIndex + SUFFIX.length()
        );
    }

    @Override
    public StorageObject createFolder(
            String path,
            String name,
            Map<String, Object> metadata) {

        String key = Optional.ofNullable(path)
                .filter(s -> !s.isEmpty())
                .filter(s -> !SUFFIX.equals(s))
                .map(this::removeLeadingSeparator)
                .map(s -> s + SUFFIX + name)
                .orElse(name);

        if (metadata != null) {
            metadata.remove(StoragePropertyNames.NAME.value());

            Object objectType = metadata.get(
                    StoragePropertyNames.OBJECT_TYPE_ID.value()
            );

            if (StoragePropertyNames.CMIS_FOLDER.value().equals(objectType)) {
                metadata.remove(StoragePropertyNames.OBJECT_TYPE_ID.value());
            }
        }

        Map<String, Object> folderMetadata = new HashMap<>();

        folderMetadata.put(
                StoragePropertyNames.BASE_TYPE_ID.value(),
                StoragePropertyNames.CMIS_FOLDER.value()
        );

        folderMetadata.put(
                StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(),
                Collections.emptyList()
        );

        folderMetadata.put(
                StoragePropertyNames.NAME.value(),
                extractName(key)
        );

        return new StorageObject(key, key, folderMetadata);
    }
    private String removeLeadingSeparator(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        String normalized = value;
        while (normalized.startsWith(SUFFIX)) {
            normalized = normalized.substring(SUFFIX.length());
        }
        return normalized;
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

    private void uploadUnknownLength(
            BlobClient blobClient,
            InputStream inputStream) {

        Path temporaryFile = null;

        try {
            temporaryFile = Files.createTempFile("azure-storage-",".upload");

            Files.copy(inputStream,temporaryFile,StandardCopyOption.REPLACE_EXISTING);
            blobClient.uploadFromFile(
                    temporaryFile.toString(),
                    true
            );
        } catch (IOException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Errore durante la preparazione del file per Azure Blob Storage",
                    e
            );
        } finally {
            if (temporaryFile != null) {
                try {
                    Files.deleteIfExists(temporaryFile);
                } catch (IOException e) {
                    LOGGER.warn(
                            "Impossibile eliminare il file temporaneo {}",
                            temporaryFile,
                            e
                    );
                }
            }
        }
    }

    @Override
    public StorageObject createDocument(
            InputStream inputStream,
            String contentType,
            Map<String, Object> metadataProperties,
            StorageObject parentObject,
            String path,
            boolean makeVersionable,
            Permission... permissions) {

        String parentPath = Optional.ofNullable(parentObject)
                .map(StorageObject::getPath)
                .map(this::removeLeadingSeparator)
                .orElseGet(() -> removeLeadingSeparator(path));

        String name = String.valueOf(
                metadataProperties.get(StoragePropertyNames.NAME.value())
        );

        String key = parentPath + SUFFIX + name;

        try {
            BlobClient blobClient =
                    blobContainerClient.getBlobClient(key);

            // Upload senza conoscere preventivamente la dimensione dell'InputStream
            uploadUnknownLength(blobClient, inputStream);

            // Recupero le properties assegnate al blob durante l'upload
            BlobProperties currentProperties =
                    blobClient.getProperties();

            BlobHttpHeaders headers = new BlobHttpHeaders()
                    .setContentType(cleanupContentType(contentType))
                    .setContentEncoding(currentProperties.getContentEncoding())
                    .setContentLanguage(currentProperties.getContentLanguage())
                    .setContentDisposition(currentProperties.getContentDisposition())
                    .setCacheControl(currentProperties.getCacheControl())
                    .setContentMd5(currentProperties.getContentMd5());

            blobClient.setHttpHeaders(headers);

            blobClient.setMetadata(
                    putUserMetadata(metadataProperties)
            );

            return new StorageObject(
                    key,
                    key,
                    getUserMetadata(blobClient)
            );

        } catch (RuntimeException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    e
            );
        }
    }
    private String getCloudBlobDirectory(StorageObject storageObject) {
        if (storageObject == null
                || storageObject.getKey() == null
                || storageObject.getKey().isBlank()) {
            return null;
        }

        String directoryPrefix =
                normalizeDirectoryPrefix(storageObject.getKey());
        try {
            boolean exists = blobContainerClient
                    .listBlobs(
                            new ListBlobsOptions()
                                    .setPrefix(directoryPrefix)
                                    .setMaxResultsPerPage(1),
                            Duration.ofSeconds(30)
                    )
                    .iterator()
                    .hasNext();
            return exists ? directoryPrefix : null;
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                LOGGER.debug(
                        "Directory {} does not exist",
                        directoryPrefix
                );

                return null;
            }
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Errore durante la ricerca della directory "
                            + directoryPrefix,
                    e
            );
        }
    }
    private void deleteBlobInDirectory(
            BlobItem blobItem,
            String directoryPrefix) {

        try {
            BlobClient blobClient =
                    blobContainerClient.getBlobClient(
                            blobItem.getName()
                    );

            boolean deleted =
                    blobClient.deleteIfExists();

            if (deleted) {
                LOGGER.debug(
                        "Deleted blob {} from directory {}",
                        blobItem.getName(),
                        directoryPrefix
                );
            } else {
                LOGGER.debug(
                        "Blob {} does not exist",
                        blobItem.getName()
                );
            }

        } catch (BlobStorageException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Errore durante la cancellazione del blob "
                            + blobItem.getName()
                            + " dalla directory "
                            + directoryPrefix,
                    e
            );
        }
    }

    private void deleteDirectory(StorageObject directory) {
        if (directory == null
                || directory.getPath() == null
                || directory.getPath().isBlank()) {
            return;
        }

        String directoryPrefix =
                getCloudBlobDirectory(directory);

        if (directoryPrefix == null) {
            LOGGER.debug(
                    "Directory {} does not exist",
                    directory.getPath()
            );

            return;
        }

        try {
            ListBlobsOptions options =
                    new ListBlobsOptions()
                            .setPrefix(directoryPrefix);

            blobContainerClient
                    .listBlobs(
                            options,
                            Duration.ofSeconds(30)
                    )
                    .forEach(blobItem -> deleteBlobInDirectory(
                            blobItem,
                            directoryPrefix
                    ));

        } catch (StorageException e) {
            throw e;
        } catch (BlobStorageException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Errore durante la cancellazione della directory "
                            + directoryPrefix,
                    e
            );
        }
    }

    private List<String> getSubDirectories(String directoryPath) {
        String directoryPrefix =
                normalizeDirectoryPrefix(directoryPath);

        try {
            ListBlobsOptions options =
                    new ListBlobsOptions()
                            .setPrefix(directoryPrefix);

            return blobContainerClient
                    .listBlobsByHierarchy(
                            SUFFIX,
                            options,
                            Duration.ofSeconds(30)
                    )
                    .stream()
                    .filter(BlobItem::isPrefix)
                    .map(BlobItem::getName)
                    .collect(Collectors.toList());

        } catch (BlobStorageException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Errore durante il recupero delle sottodirectory di "
                            + directoryPrefix,
                    e
            );
        }
    }
    private void rollbackDirectoryCopy(StorageObject target) {
        try {
            deleteDirectory(target);
        } catch (StorageException rollbackException) {
            LOGGER.error(
                    "Errore durante il rollback della directory {}",
                    target.getPath(),
                    rollbackException
            );
        }
    }
    private void validateDirectoryCopy(
            StorageObject source,
            StorageObject target) {

        if (source == null) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "La directory sorgente non può essere null"
            );
        }

        if (target == null) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "La directory destinazione non può essere null"
            );
        }

        if (source.getPath() == null
                || source.getPath().isBlank()) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Il path della directory sorgente non può essere vuoto"
            );
        }

        if (target.getPath() == null
                || target.getPath().isBlank()) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Il path della directory destinazione non può essere vuoto"
            );
        }
    }
    private BlobHttpHeaders createBlobHttpHeaders(
            BlobProperties properties) {

        BlobHttpHeaders headers =
                new BlobHttpHeaders()
                        .setContentType(properties.getContentType())
                        .setContentEncoding(properties.getContentEncoding())
                        .setContentLanguage(properties.getContentLanguage())
                        .setContentDisposition(properties.getContentDisposition())
                        .setCacheControl(properties.getCacheControl());
        if (properties.getContentMd5() != null) {
            headers.setContentMd5(
                    properties.getContentMd5()
            );
        }
        return headers;
    }

    private void copyBlob(
            String sourceKey,
            String targetKey) {

        BlobClient sourceBlob =
                blobContainerClient.getBlobClient(sourceKey);

        BlobClient targetBlob =
                blobContainerClient.getBlobClient(targetKey);

        try {
            BlobProperties sourceProperties =
                    sourceBlob.getProperties();

            try (InputStream inputStream =
                         sourceBlob.openInputStream()) {

                targetBlob.upload(
                        inputStream,
                        sourceProperties.getBlobSize(),
                        true
                );
            }

            Map<String, String> metadata =
                    sourceProperties.getMetadata();

            if (metadata != null && !metadata.isEmpty()) {
                targetBlob.setMetadata(
                        new HashMap<>(metadata)
                );
            }

            BlobHttpHeaders headers =
                    createBlobHttpHeaders(sourceProperties);

            targetBlob.setHttpHeaders(headers);

            LOGGER.debug(
                    "Copied blob from {} to {}",
                    sourceKey,
                    targetKey
            );

        } catch (IOException | RuntimeException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Errore durante la copia del blob da "
                            + sourceKey
                            + " a "
                            + targetKey,
                    e
            );
        }
    }
    private void copyDirectory(
            StorageObject source,
            StorageObject target) {

        validateDirectoryCopy(source, target);

        String sourcePath =
                sanitazeDirectoryPath(source.getPath());

        String targetPath =
                sanitazeDirectoryPath(target.getPath());

        if (sourcePath.equalsIgnoreCase(targetPath)) {
            return;
        }

        String sourcePrefix =
                getCloudBlobDirectory(source);

        if (sourcePrefix == null) {
            throw new StorageException(
                    StorageException.Type.NOT_FOUND,
                    "Directory sorgente non trovata: "
                            + source.getPath()
            );
        }

        String targetPrefix =
                normalizeDirectoryPrefix(target.getPath());

        try {
            ListBlobsOptions options =
                    new ListBlobsOptions()
                            .setPrefix(sourcePrefix);

            blobContainerClient
                    .listBlobs(
                            options,
                            Duration.ofSeconds(30)
                    )
                    .forEach(blobItem -> {
                        String relativePath =
                                blobItem.getName().substring(
                                        sourcePrefix.length()
                                );

                        String targetKey =
                                targetPrefix + relativePath;

                        copyBlob(
                                blobItem.getName(),
                                targetKey
                        );
                    });

        } catch (StorageException e) {
            rollbackDirectoryCopy(target);
            throw e;

        } catch (BlobStorageException e) {
            rollbackDirectoryCopy(target);

            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Errore durante la copia della directory da "
                            + sourcePrefix
                            + " a "
                            + targetPrefix,
                    e
            );
        }
    }

    private boolean isDirectory(StorageObject storageObject) {
        return getCloudBlobDirectory(storageObject) != null;
    }
    private String removeTrailingSeparator(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }

        String normalized = value;

        while (normalized.endsWith(SUFFIX)) {
            normalized = normalized.substring(
                    0,
                    normalized.length() - SUFFIX.length()
            );
        }

        return normalized;
    }

    private String normalizeDirectoryPrefix(String path) {
        String normalized = sanitazeDirectoryPath(path);

        if (normalized == null || normalized.isBlank()) {
            return "";
        }

        return normalized + SUFFIX;
    }


    @Override
    public void updateProperties(
            StorageObject storageObject,
            Map<String, Object> metadataProperties) {

        if (storageObject == null) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "StorageObject non può essere null"
            );
        }

        if (isDirectory(storageObject)) {
            Object newName = metadataProperties.get(
                    StoragePropertyNames.NAME.value()
            );

            if (newName != null) {
                String parentPath = getParentPath(storageObject.getPath());
                String newPath = parentPath.isEmpty()
                        ? String.valueOf(newName)
                        : parentPath + SUFFIX + newName;

                renameDirectory(
                        storageObject,
                        getObjectByPath(newPath, true)
                );
            }

            return;
        }

        try {
            BlobClient blobClient = blobContainerClient.getBlobClient(
                    removeLeadingSeparator(storageObject.getKey())
            );

            if (!blobClient.exists()) {
                return;
            }

            Map<String, String> mergedMetadata =
                    new HashMap<>(blobClient.getProperties().getMetadata());

            putUserMetadata(metadataProperties).forEach(
                    (key, value) -> mergedMetadata.merge(
                            key,
                            value,
                            (oldValue, newValue) -> newValue
                    )
            );

            blobClient.setMetadata(mergedMetadata);

        } catch (RuntimeException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    e
            );
        }
    }

    private String getParentPath(String path) {
        String normalized = sanitazeDirectoryPath(path);

        if (normalized == null) {
            return "";
        }

        int separator = normalized.lastIndexOf(SUFFIX);

        return separator >= 0
                ? normalized.substring(0, separator)
                : "";
    }

    @Override
    public StorageObject updateStream(
            String key,
            InputStream inputStream,
            String contentType) {

        String normalizedKey = removeLeadingSeparator(key);

        try {
            BlobClient blobClient =
                    blobContainerClient.getBlobClient(normalizedKey);

            Map<String, String> existingMetadata = blobClient.exists()
                    ? new HashMap<>(blobClient.getProperties().getMetadata())
                    : Collections.emptyMap();

            BlockBlobClient blockBlobClient =
                    blobClient.getBlockBlobClient();

            try (BlobOutputStream outputStream =
                         blockBlobClient.getBlobOutputStream(true)) {

                inputStream.transferTo(outputStream);
            }

            if (!existingMetadata.isEmpty()) {
                blobClient.setMetadata(existingMetadata);
            }

            blobClient.setHttpHeaders(new BlobHttpHeaders().setContentType(cleanupContentType(contentType))
            );

            return new StorageObject(
                    normalizedKey,
                    normalizedKey,
                    getUserMetadata(blobClient)
            );

        } catch (IOException | RuntimeException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    e
            );
        }
    }

    @Override
    public InputStream getInputStream(String key) {
        String normalizedKey = Optional.ofNullable(key)
                .map(this::removeLeadingSeparator)
                .orElseThrow(() -> new StorageException(
                        StorageException.Type.NOT_FOUND,
                        "Key is null"
                ));

        try {
            BlobClient blobClient =
                    blobContainerClient.getBlobClient(normalizedKey);

            if (!blobClient.exists()) {
                throw new StorageException(
                        StorageException.Type.NOT_FOUND,
                        "Blob non trovato: " + normalizedKey
                );
            }

            return blobClient.openInputStream();

        } catch (StorageException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    e
            );
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
        String normalizedKey = removeLeadingSeparator(key);

        try {
            BlobClient blobClient =
                    blobContainerClient.getBlobClient(normalizedKey);

            return blobClient.deleteIfExists();

        } catch (RuntimeException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    e
            );
        }
    }



    @Override
    public StorageObject getObject(String key) {
        String normalizedKey = sanitazeDirectoryPath(key);

        if (normalizedKey == null) {
            return null;
        }

        BlobClient blobClient =
                blobContainerClient.getBlobClient(normalizedKey);

        try {
            if (blobClient.exists()) {
                return new StorageObject(
                        normalizedKey,
                        normalizedKey,
                        getUserMetadata(blobClient)
                );
            }

            StorageObject possibleDirectory = new StorageObject(
                    normalizedKey,
                    normalizedKey,
                    Collections.emptyMap()
            );

            if (isDirectory(possibleDirectory)) {
                return createDirectoryStorageObject(normalizedKey);
            }

            return null;

        } catch (RuntimeException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    e
            );
        }
    }

    private StorageObject createDirectoryStorageObject(String path) {
        Map<String, Object> metadata = new HashMap<>();

        metadata.put(
                StoragePropertyNames.BASE_TYPE_ID.value(),
                StoragePropertyNames.CMIS_FOLDER.value()
        );

        metadata.put(
                StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(),
                Collections.emptyList()
        );

        metadata.put(
                StoragePropertyNames.NAME.value(),
                extractName(path)
        );

        return new StorageObject(path, path, metadata);
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


    @Override
    public List<StorageObject> getChildren(String key) {
        String prefix = normalizeDirectoryPrefix(key);

        List<StorageObject> children = new ArrayList<>();

        try {
            Iterable<BlobItem> blobItems =
                    blobContainerClient.listBlobsByHierarchy(
                            SUFFIX,
                            new ListBlobsOptions()
                                    .setPrefix(prefix)
                                    .setDetails(
                                            new BlobListDetails()
                                                    .setRetrieveMetadata(true)
                                    ),
                            Duration.ofSeconds(30)
                    );

            for (BlobItem item : blobItems) {
                if (item.isPrefix()) {
                    String directoryPath =
                            sanitazeDirectoryPath(item.getName());

                    children.add(
                            createDirectoryStorageObject(directoryPath)
                    );
                } else {
                    BlobClient blobClient =
                            blobContainerClient.getBlobClient(item.getName());

                    children.add(
                            new StorageObject(
                                    item.getName(),
                                    item.getName(),
                                    getUserMetadata(blobClient)
                            )
                    );
                }
            }

            return children;

        } catch (RuntimeException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    e
            );
        }
    }
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

    private String sanitazeDirectoryPath(String path) {
        if (path == null || path.isEmpty() || SUFFIX.equals(path)) {
            return path;
        }

        String normalized = path;

        while (normalized.startsWith(SUFFIX)) {
            normalized = normalized.substring(SUFFIX.length());
        }

        while (normalized.endsWith(SUFFIX)) {
            normalized = normalized.substring(
                    0,
                    normalized.length() - SUFFIX.length()
            );
        }

        return normalized;
    }

    private String getParentDirectoryPath(String path) {
        String normalized =
                sanitazeDirectoryPath(path);

        if (normalized == null || normalized.isEmpty()) {
            return "";
        }

        int index = normalized.lastIndexOf(SUFFIX);

        return index >= 0
                ? normalized.substring(0, index)
                : "";
    }

    private boolean isValidPathRenameDirectory(
            StorageObject source,
            StorageObject target) {

        if (source == null
                || source.getPath() == null
                || source.getPath().isBlank()) {
            return false;
        }

        if (target == null
                || target.getPath() == null
                || target.getPath().isBlank()) {
            return false;
        }

        String sourceParent =
                getParentDirectoryPath(source.getPath());

        String targetParent =
                getParentDirectoryPath(target.getPath());

        return sourceParent.equals(targetParent);
    }

    private void renameDirectory(
            StorageObject source,
            StorageObject target) {

        if (source == null
                || source.getPath() == null
                || source.getPath().isBlank()) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "RenameDirectory: source directory non valida"
            );
        }

        if (target == null
                || target.getPath() == null
                || target.getPath().isBlank()) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "RenameDirectory: target directory non valida"
            );
        }

        if (!isValidPathRenameDirectory(source, target)) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Le directory source e target devono avere lo stesso parent: "
                            + source.getPath()
                            + ", "
                            + target.getPath()
            );
        }

        if (source.getPath().equalsIgnoreCase(
                target.getPath()
        )) {
            return;
        }

        copyDirectory(source, target);
        deleteDirectory(source);
    }



    private void validateCopyArguments(
            StorageObject source,
            StorageObject target) {

        if (source == null) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Source non può essere null"
            );
        }

        if (target == null) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Target non può essere null"
            );
        }

        if (source.getKey() == null || source.getKey().isBlank()) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "La key del source non può essere vuota"
            );
        }

        if (target.getPath() == null) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Il path del target non può essere null"
            );
        }
    }

    private void copyBlobContent(
            BlobClient sourceBlob,
            BlobClient targetBlob,
            long contentLength) {

        try (InputStream sourceStream = sourceBlob.openInputStream()) {

            targetBlob.upload(
                    sourceStream,
                    contentLength,
                    true
            );

        } catch (IOException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Errore durante la lettura del blob sorgente: "
                            + sourceBlob.getBlobName(),
                    e
            );
        }
    }
    private void copyBlobMetadata(
            BlobProperties sourceProperties,
            BlobClient targetBlob) {

        Map<String, String> metadata =
                sourceProperties.getMetadata();

        if (metadata == null || metadata.isEmpty()) {
            return;
        }

        targetBlob.setMetadata(
                new HashMap<>(metadata)
        );
    }
    private void copyBlobHeaders(
            BlobProperties sourceProperties,
            BlobClient targetBlob) {

        BlobHttpHeaders headers = new BlobHttpHeaders()
                .setContentType(sourceProperties.getContentType())
                .setContentEncoding(sourceProperties.getContentEncoding())
                .setContentLanguage(sourceProperties.getContentLanguage())
                .setContentDisposition(sourceProperties.getContentDisposition())
                .setCacheControl(sourceProperties.getCacheControl());

        /*
         * Impostiamo il Content-MD5 soltanto se presente.
         * Un valore null non deve essere forzato sul blob di destinazione.
         */
        if (sourceProperties.getContentMd5() != null) {
            headers.setContentMd5(
                    sourceProperties.getContentMd5()
            );
        }

        targetBlob.setHttpHeaders(headers);
    }

    @Override
    public void copyNode(StorageObject source, StorageObject target) {
        validateCopyArguments(source, target);

        String sourceKey = removeLeadingSeparator(source.getKey());

        String fileName = Optional.ofNullable(
                        source.getPropertyValue(StoragePropertyNames.NAME.value())
                )
                .map(String::valueOf)
                .filter(name -> !name.isBlank())
                .orElseGet(() -> extractName(sourceKey));

        String targetKey =
                normalizeDirectoryPrefix(target.getPath()) + fileName;

        BlobClient sourceBlob =
                blobContainerClient.getBlobClient(sourceKey);

        BlobClient targetBlob =
                blobContainerClient.getBlobClient(targetKey);

        try {
            if (!sourceBlob.exists()) {
                throw new StorageException(
                        StorageException.Type.NOT_FOUND,
                        "Blob sorgente non trovato: " + sourceKey
                );
            }

            BlobProperties sourceProperties =
                    sourceBlob.getProperties();

            copyBlobContent(
                    sourceBlob,
                    targetBlob,
                    sourceProperties.getBlobSize()
            );

            copyBlobMetadata(sourceProperties, targetBlob);
            copyBlobHeaders(sourceProperties, targetBlob);

        } catch (StorageException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new StorageException(
                    StorageException.Type.GENERIC,
                    "Errore durante la copia da "
                            + sourceKey
                            + " a "
                            + targetKey,
                    e
            );
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
