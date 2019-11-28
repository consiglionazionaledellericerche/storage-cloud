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

import org.apache.http.auth.UsernamePasswordCredentials;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Created by mspasiano on 6/5/17.
 */
public interface StorageService {


    String SUFFIX = "/";
    Pattern UUID_PATTERN = Pattern.compile("[a-f0-9]{8}-[a-f0-9]{4}-[12345][a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}");

    /**
     * create a new folder
     *
     * @param path     folder path
     * @param name     name of folder
     * @param metadata object metadata
     * @return StorageObject
     */
    StorageObject createFolder(String path, String name, Map<String, Object> metadata);

    /**
     * create a new document
     *
     * @param inputStream        stream of document
     * @param contentType        content type
     * @param metadataProperties metadataProperties
     * @param parentObject       parentObject
     * @param path               path
     * @param makeVersionable    makeVersionable
     * @param permissions        permissions
     * @return StorageObject
     */
    StorageObject createDocument(InputStream inputStream, String contentType, Map<String, Object> metadataProperties, StorageObject parentObject, String path, boolean makeVersionable, Permission... permissions);

    /**
     * properties of store object
     *
     * @param storageObject      storageObject
     * @param metadataProperties metadataProperties
     */
    void updateProperties(StorageObject storageObject, Map<String, Object> metadataProperties);

    /**
     * Update stream of document
     *
     * @param key         key
     * @param inputStream inputStream
     * @param contentType contentType
     * @return StorageObject StorageObject
     */
    StorageObject updateStream(String key, InputStream inputStream, String contentType);

    /**
     * get object input stream
     *
     * @param name object name
     * @return object InputStream
     */
    InputStream getInputStream(String name);

    /**
     * get object input stream for specified version
     *
     * @param key       key
     * @param versionId versionId
     * @return object InputStream
     */
    InputStream getInputStream(String key, String versionId);

    /**
     * get object input stream for major version
     *
     * @param key          key
     * @param majorVersion majorVersion
     * @return object InputStream
     */
    InputStream getInputStream(String key, Boolean majorVersion);

    /**
     * delete an object
     *
     * @param id object id
     * @return a CompletableFuture wrapping true if object exists
     */
    Boolean delete(String id);

    /**
     * @param id object id
     * @return StorageObject
     */
    StorageObject getObject(String id);

    /**
     * @param id                object id
     * @param customCredentials credential
     * @return StorageObject
     */
    StorageObject getObject(String id, UsernamePasswordCredentials customCredentials);

    /**
     * @param path     object path
     * @param isFolder isFolder
     * @return StorageObject
     */
    StorageObject getObjectByPath(String path, boolean isFolder);

    /**
     * retrieve all children
     *
     * @param key object id
     * @return list of StorageObject
     */
    List<StorageObject> getChildren(String key);

    /**
     * retrieve all children
     *
     * @param key   object id
     * @param depth depth
     * @return list of StorageObject
     */
    List<StorageObject> getChildren(String key, int depth);

    /**
     * search documents or folders
     *
     * @param query query statement
     * @return list of StorageObject
     */
    List<StorageObject> search(String query);

    /**
     * Sign documents
     *
     * @param json json
     * @param url  url
     * @return message error or null
     */
    String signDocuments(String json, String url);

    /**
     * Link object
     *
     * @param source source
     * @param target target
     */
    void copyNode(StorageObject source, StorageObject target);

    /**
     * Manage permission
     *
     * @param storageObject storageObject
     * @param permission    permission
     * @param remove        remove
     */
    void managePermission(StorageObject storageObject, Map<String, ACLType> permission, boolean remove);

    /**
     * @param storageObject storageObject
     * @param inherited     inherited
     */
    void setInheritedPermission(StorageObject storageObject, Boolean inherited);

    /**
     * @param key              key
     * @param relationshipName relationshipName
     * @param fromTarget       fromTarget
     * @return list of StorageObject
     */
    List<StorageObject> getRelationship(String key, String relationshipName, boolean fromTarget);

    /**
     * @param source           source
     * @param target           target
     * @param relationshipName relationshipName
     */
    void createRelationship(String source, String target, String relationshipName);

    StoreType getStoreType();

    /**
     * Inizialize service
     */
    void init();

    default boolean isUUID(String uuid) {
        return UUID_PATTERN.matcher(uuid).matches();
    }

    enum StoreType {
        CMIS, S3, AZURE, FILESYSTEM
    }

    enum ACLType {
        Consumer, Editor, Collaborator, Coordinator, Contributor, FullControl, Read, Write
    }

    class Permission {
        private String userName;
        private ACLType role;

        protected Permission(String userName, ACLType role) {
            super();
            this.userName = userName;
            this.role = role;
        }

        public static Permission construct(String userName, ACLType role) {
            return new Permission(userName, role);
        }

        public static Map<String, ACLType> convert(Permission... permissions) {
            return Stream.of(permissions)
                    .collect(HashMap::new, (m, v) -> m.put(v.getUserName(), v.getRole()), HashMap::putAll);
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public ACLType getRole() {
            return role;
        }

        public void setRole(ACLType role) {
            this.role = role;
        }

    }

}
