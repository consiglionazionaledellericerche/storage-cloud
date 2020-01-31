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

import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by mspasiano on 6/12/17.
 */
@Service
@DependsOn("storageDriverConfigurationChecker")
public class StoreService {
    @Autowired
    private StorageDriver storageDriver;
    @Autowired
    private StoreBulkInfo storeBulkInfo;

    public String sanitizeFilename(String name) {
        name = name.trim();
        Pattern pattern = Pattern.compile("([\\/:@()&<>?\"])");
        Matcher matcher = pattern.matcher(name);
        if (!matcher.matches()) {
            String str1 = matcher.replaceAll("_");
            return str1;
        } else {
            return name;
        }
    }

    public String sanitizeFolderName(String name) {
        name = name.trim();
        Pattern pattern = Pattern.compile("([\\/:@()&<>?\"])");
        Matcher matcher = pattern.matcher(name);
        if (!matcher.matches()) {
            String str1 = matcher.replaceAll("'");
            return str1;
        } else {
            return name;
        }
    }

    public StorageObject getStorageObjectByPath(String path, boolean isFolder, boolean create) {
        return Optional.ofNullable(storageDriver.getObjectByPath(path, isFolder))
                .orElseGet(() -> {
                    if (!create) return null;
                    final List<String> names = Arrays.asList(path.split(StorageDriver.SUFFIX));
                    AtomicInteger atomicInteger = new AtomicInteger(0);
                    names.stream()
                            .filter(name -> name.length() > 0)
                            .forEach(name -> {
                                atomicInteger.getAndIncrement();
                                createFolderIfNotPresent(
                                        Optional.ofNullable(names.stream()
                                                .limit(atomicInteger.longValue())
                                                .reduce((a, b) -> a + StorageDriver.SUFFIX + b)
                                                .get())
                                                .filter(s -> s.length() > 0)
                                                .orElse(StorageDriver.SUFFIX)
                                        , name, null, null);
                            });
                    if (create) {
                        return Optional.ofNullable(storageDriver.getObjectByPath(path, true))
                                .orElse(new StorageObject(path, path, Collections.emptyMap()));
                    }
                    return storageDriver.getObjectByPath(path, true);
                });
    }

    public StorageObject getStorageObjectByPath(String path, boolean create) {
        return getStorageObjectByPath(path, false, create);
    }

    public StorageObject getStorageObjectByPath(String path) {
        return getStorageObjectByPath(path, false);
    }

    public StorageObject getStorageObjectBykey(String key) {
        return storageDriver.getObject(key);
    }

    public StorageObject getStorageObjectBykey(String key, UsernamePasswordCredentials customCredentials) {
        return storageDriver.getObject(key, customCredentials);
    }

    public String createFolderIfNotPresent(String path, String folderName, String title, String description) {
        return createFolderIfNotPresent(path, folderName, title, description, null);
    }

    public String createFolderIfNotPresent(String path, String folderName, String title, String description, Serializable oggettoBulk) {
        return createFolderIfNotPresent(path, folderName, title, description, oggettoBulk, null);
    }

    public String updateMetadataFromBulk(StorageObject storageObject, Serializable oggettoBulk) throws StorageException {
        if (oggettoBulk != null) {
            Map<String, Object> metadataProperties = new HashMap<String, Object>();
            List<String> aspectsToAdd = new ArrayList<String>();
            List<String> aspects = (List<String>) storageObject.getPropertyValue(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value());
            Optional.ofNullable(storeBulkInfo.getType(oggettoBulk))
                    .ifPresent(type -> metadataProperties.put(StoragePropertyNames.OBJECT_TYPE_ID.value(), type));
            metadataProperties.putAll(storeBulkInfo.getPropertyValue(oggettoBulk));
            aspectsToAdd.addAll(storeBulkInfo.getAspect(oggettoBulk));
            metadataProperties.putAll(storeBulkInfo.getAspectPropertyValue(oggettoBulk));
            aspects.addAll(aspectsToAdd);
            metadataProperties.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), aspects);
            storageDriver.updateProperties(storageObject, metadataProperties);
        }
        return storageObject.getPath();
    }

    public String createFolderIfNotPresent(String path, String folderName, String title, String description, Serializable oggettoBulk, String objectTypeName) throws StorageException {
        Map<String, Object> metadataProperties = new HashMap<String, Object>();
        List<String> aspectsToAdd = new ArrayList<String>();
        try {
            final StorageObject parentObject = getStorageObjectByPath(path, true, true);
            final String name = sanitizeFolderName(folderName);
            metadataProperties.put(StoragePropertyNames.NAME.value(), name);
            if (title != null || description != null) {
                aspectsToAdd.add(StoragePropertyNames.ASPECT_TITLED.value());
                metadataProperties.put(StoragePropertyNames.TITLE.value(), title);
                metadataProperties.put(StoragePropertyNames.DESCRIPTION.value(), description);
            }
            if (oggettoBulk != null) {
                metadataProperties.put(StoragePropertyNames.OBJECT_TYPE_ID.value(), storeBulkInfo.getType(oggettoBulk));
                metadataProperties.putAll(storeBulkInfo.getPropertyValue(oggettoBulk));
                aspectsToAdd.addAll(storeBulkInfo.getAspect(oggettoBulk));
                metadataProperties.putAll(storeBulkInfo.getAspectPropertyValue(oggettoBulk));
            } else {
                metadataProperties.put(StoragePropertyNames.OBJECT_TYPE_ID.value(), StoragePropertyNames.CMIS_FOLDER.value());
            }
            Optional.ofNullable(aspectsToAdd)
                    .filter(list -> !list.isEmpty())
                    .ifPresent(list -> {
                        metadataProperties.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), list);
                    });
            return createFolderIfNotPresent(parentObject.getPath(), name, metadataProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String createFolderIfNotPresent(String path, String name, Map<String, Object> metadataProperties) {
        return Optional.ofNullable(storageDriver.getObjectByPath(
                path.concat(path.equals(StorageDriver.SUFFIX) ? "" : StorageDriver.SUFFIX).concat(name), true
        ))
                .map(StorageObject::getPath)
                .orElseGet(() -> storageDriver.createFolder(path, name, metadataProperties).getPath());
    }

    public Boolean delete(StorageObject storageObject) {
        return delete(storageObject.getKey());
    }

    public Boolean delete(String key) {
        Assert.notNull(key);
        return storageDriver.delete(key);
    }

    public InputStream getResource(StorageObject storageObject) {
        return getResource(storageObject.getKey());
    }

    public InputStream getResource(String key) {
        return storageDriver.getInputStream(key);
    }

    public InputStream getResource(String key, String versionId) {
        return storageDriver.getInputStream(key, versionId);
    }

    public InputStream getResource(String key, Boolean majorVersion) {
        return storageDriver.getInputStream(key, majorVersion);
    }

    public StorageObject storeSimpleDocument(Serializable oggettoBulk, InputStream inputStream, String contentType, String name,
                                             String path, StorageDriver.Permission... permissions) throws StorageException {
        return storeSimpleDocument(oggettoBulk, inputStream, contentType, name, path, false, permissions);
    }

    public StorageObject storeSimpleDocument(InputStream inputStream, String contentType, String path, Map<String, Object> metadataProperties) throws StorageException {
        StorageObject parentObject = getStorageObjectByPath(path, true, true);
        return storeSimpleDocument(inputStream, contentType, metadataProperties, parentObject);
    }

    public StorageObject storeSimpleDocument(InputStream inputStream, String contentType, Map<String, Object> metadataProperties, StorageObject parentObject) throws StorageException {
        return storageDriver.createDocument(inputStream, contentType, metadataProperties, parentObject, parentObject.getPath(), false);
    }

    public StorageObject storeSimpleDocument(Serializable oggettoBulk, InputStream inputStream, String contentType, String name,
                                             String path, boolean makeVersionable, StorageDriver.Permission... permissions) throws StorageException {
        return storeSimpleDocument(oggettoBulk, inputStream, contentType, name, path, storeBulkInfo.getType(oggettoBulk), makeVersionable, permissions);
    }

    public StorageObject storeSimpleDocument(Serializable oggettoBulk, InputStream inputStream, String contentType, String name,
                                             String path, String objectTypeName, boolean makeVersionable, StorageDriver.Permission... permissions) throws StorageException {
        StorageObject parentObject = getStorageObjectByPath(path, true, true);
        Map<String, Object> metadataProperties = new HashMap<String, Object>();
        name = sanitizeFilename(name);
        metadataProperties.put(StoragePropertyNames.NAME.value(), name);
        metadataProperties.put(StoragePropertyNames.OBJECT_TYPE_ID.value(), objectTypeName);
        metadataProperties.putAll(storeBulkInfo.getPropertyValue(oggettoBulk));
        metadataProperties.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(),
                Optional.ofNullable(metadataProperties.get(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value()))
                        .map(o -> (List<String>) o)
                        .map(aspects -> {
                            aspects.addAll(storeBulkInfo.getAspect(oggettoBulk));
                            return aspects;
                        })
                        .orElse(storeBulkInfo.getAspect(oggettoBulk))
        );
        metadataProperties.putAll(storeBulkInfo.getAspectPropertyValue(oggettoBulk));
        return storageDriver.createDocument(inputStream, contentType, metadataProperties, parentObject, path, makeVersionable, permissions);
    }

    public StorageObject restoreSimpleDocument(Serializable oggettoBulk, InputStream inputStream, String contentType, String name,
                                               String path, boolean makeVersionable, StorageDriver.Permission... permissions) throws StorageException {
        return restoreSimpleDocument(oggettoBulk, inputStream, contentType, name, path, storeBulkInfo.getType(oggettoBulk), makeVersionable, permissions);
    }

    public StorageObject restoreSimpleDocument(Serializable oggettoBulk, InputStream inputStream, String contentType, String name,
                                               String path, String objectTypeName, boolean makeVersionable, StorageDriver.Permission... permissions) throws StorageException {
        Optional<StorageObject> optStorageObject = Optional.ofNullable(getStorageObjectByPath(path.concat(StorageDriver.SUFFIX).concat(sanitizeFilename(name))));
        if (optStorageObject.isPresent()) {
            return storageDriver.updateStream(optStorageObject.get().getKey(), inputStream, contentType);
        } else {
            return storeSimpleDocument(oggettoBulk, inputStream, contentType, name, path, objectTypeName, makeVersionable, permissions);
        }
    }

    public void updateProperties(Map<String, Object> metadataProperties, StorageObject storageObject) throws StorageException {
        storageDriver.updateProperties(storageObject, metadataProperties);
    }

    public void updateProperties(Serializable oggettoBulk, StorageObject storageObject) throws StorageException {
        Map<String, Object> metadataProperties = new HashMap<String, Object>();
        metadataProperties.put(StoragePropertyNames.OBJECT_TYPE_ID.value(), storeBulkInfo.getType(oggettoBulk));
        metadataProperties.putAll(storeBulkInfo.getPropertyValue(oggettoBulk));
        metadataProperties.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(),
                Optional.ofNullable(metadataProperties.get(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value()))
                        .map(o -> (List<String>) o)
                        .map(aspects -> {
                            aspects.addAll(storeBulkInfo.getAspect(oggettoBulk));
                            return aspects;
                        })
                        .orElse(storeBulkInfo.getAspect(oggettoBulk))
        );
        metadataProperties.putAll(storeBulkInfo.getAspectPropertyValue(oggettoBulk));
        updateProperties(metadataProperties, storageObject);
    }

    public List<StorageObject> getChildren(String key) {
        return storageDriver.getChildren(key);
    }

    public List<StorageObject> getChildren(String key, int depth) {
        return storageDriver.getChildren(key, depth);
    }

    public List<StorageObject> search(String query) {
        return storageDriver.search(query);
    }


    private String signDocuments(String json, String url) throws StorageException {
        return storageDriver.signDocuments(json, url);
    }

    public StorageObject updateStream(String key, InputStream inputStream, String contentType) throws StorageException {
        return storageDriver.updateStream(key, inputStream, contentType);
    }

    public boolean hasAspect(StorageObject storageObject, String aspect) {
        return storageObject.<List<String>>getPropertyValue(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value()).contains(aspect);
    }

    public void addAspect(StorageObject storageObject, String aspect) {
        List<String> aspects =
                Optional.ofNullable(storageObject.<List<String>>getPropertyValue(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value()))
                        .orElse(new ArrayList<String>());
        aspects.add(aspect);
        updateProperties(Collections.singletonMap(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), aspects), storageObject);
    }

    public void removeAspect(StorageObject storageObject, String aspect) {
        List<String> aspects =
                Optional.ofNullable(storageObject.<List<String>>getPropertyValue(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value()))
                        .orElse(new ArrayList<String>());
        aspects.remove(aspect);
        updateProperties(Collections.singletonMap(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), aspects), storageObject);
    }

    public void copyNode(StorageObject source, StorageObject target) {
        storageDriver.copyNode(source, target);
    }

    public void addConsumerToEveryone(StorageObject storageObject) {
        addAcl(storageObject, Collections.singletonMap("GROUP_EVERYONE", StorageDriver.ACLType.Consumer));
    }

    public void removeConsumerToEveryone(StorageObject storageObject) {
        removeAcl(storageObject, Collections.singletonMap("GROUP_EVERYONE", StorageDriver.ACLType.Consumer));
    }

    // per gestire gruppi diversi es. CONTRATTI
    public void addConsumer(StorageObject storageObject, String group) {
        addAcl(storageObject, Collections.singletonMap(group, StorageDriver.ACLType.Consumer));
    }

    public void removeConsumer(StorageObject storageObject, String group) {
        removeAcl(storageObject, Collections.singletonMap(group, StorageDriver.ACLType.Consumer));
    }

    private void removeAcl(StorageObject storageObject, Map<String, StorageDriver.ACLType> permission) {
        managePermission(storageObject, permission, true);
    }

    private void addAcl(StorageObject storageObject, Map<String, StorageDriver.ACLType> permission) {
        managePermission(storageObject, permission, false);
    }

    private void managePermission(StorageObject storageObject, Map<String, StorageDriver.ACLType> permission, boolean remove) {
        storageDriver.managePermission(storageObject, permission, remove);
    }

    public void setInheritedPermission(StorageObject storageObject, Boolean inherited) {
        storageDriver.setInheritedPermission(storageObject, inherited);
    }

    public List<StorageObject> getRelationship(String key, String relationshipName, boolean fromTarget) {
        return storageDriver.getRelationship(key, relationshipName, fromTarget);
    }

    public List<StorageObject> getRelationship(String sourceNodeRef, String relationshipName) {
        return getRelationship(sourceNodeRef, relationshipName, false);
    }

    public List<StorageObject> getRelationshipFromTarget(String sourceNodeRef, String relationshipName) {
        return getRelationship(sourceNodeRef, relationshipName, true);
    }

    public void createRelationship(String source, String target, String relationshipName) {
        storageDriver.createRelationship(source, target, relationshipName);
    }
}
