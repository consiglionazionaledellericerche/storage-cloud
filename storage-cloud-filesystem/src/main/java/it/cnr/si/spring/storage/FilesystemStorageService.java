package it.cnr.si.spring.storage;

import it.cnr.si.spring.storage.StorageObject;
import it.cnr.si.spring.storage.StorageService;
import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by mtrycz on 08/11/2019
 */

@Service
public class FilesystemStorageService implements StorageService{

    private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemStorageService.class);

    @Value("${cnr.filesystem.directory}")
    private String directory;

    @Override
    public void init() {
        try {
            Files.createDirectories(Paths.get(directory));
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Init: unable to create base storage directory "+ directory, e);
        }
    }

    @Override
    public StorageObject createFolder(String path, String name, Map<String, Object> metadata) {
        Path p = Paths.get(directory, path, name);
        try {
            Files.createDirectories(p);
            saveMetadata(p, metadata);
            return new StorageObject(p.toString(), p.toString(), metadata);
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Unable to create directory "+ p, e);
        }
    }

    @Override
    public StorageObject createDocument(InputStream inputStream,
                                        String contentType,
                                        Map<String, Object> metadataProperties,
                                        StorageObject parentObject,
                                        String path,
                                        boolean makeVersionable,
                                        StorageService.Permission... permissions) {

        if (makeVersionable)
            LOGGER.debug("Filesystem storage is meant for testing only. Versionable logic is ignored");
        if (permissions.length > 0)
            LOGGER.debug("Filesystem storage is meant for testing only. Permission logic is ignored");

        String filename = UUID.randomUUID().toString();
        Path p = Paths.get(
                parentObject == null ? parentObject.getPath() : directory + path,
                filename);

        try {

            Files.copy(inputStream, p);
            inputStream.close();

            saveMetadata(p, metadataProperties);
            return new StorageObject(p.toString(), p.toString(), metadataProperties);
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Unable to create file "+ p, e);
        }
    }

    @Override
    public void updateProperties(StorageObject storageObject, Map<String, Object> metadataProperties) {

    }

    @Override
    public StorageObject updateStream(String key, InputStream inputStream, String contentType) {
        return null;
    }

    @Override
    public InputStream getInputStream(String name) {
        try {
            return Files.newInputStream(Paths.get(name));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream getInputStream(String key, String versionId) {
        return null;
    }

    @Override
    public InputStream getInputStream(String key, Boolean majorVersion) {
        return null;
    }

    @Override
    public Boolean delete(String id) {
        return null;
    }

    @Override
    public StorageObject getObject(String id) {
        throw new NotImplementedException();
    }

    @Override
    public StorageObject getObject(String id, UsernamePasswordCredentials customCredentials) {
        throw new NotImplementedException();
    }

    @Override
    public StorageObject getObjectByPath(String path, boolean isFolder) {
        return null;
    }

    @Override
    public List<StorageObject> getChildren(String key) {
        return null;
    }

    @Override
    public List<StorageObject> getChildren(String key, int depth) {
        return null;
    }

    @Override
    public List<StorageObject> search(String query) {
        return null;
    }

    @Override
    public String signDocuments(String json, String url) {
        return null;
    }

    @Override
    public void copyNode(StorageObject source, StorageObject target) {

    }

    @Override
    public void managePermission(StorageObject storageObject, Map<String, ACLType> permission, boolean remove) {

    }

    @Override
    public void setInheritedPermission(StorageObject storageObject, Boolean inherited) {

    }

    @Override
    public List<StorageObject> getRelationship(String key, String relationshipName, boolean fromTarget) {
        return null;
    }

    @Override
    public void createRelationship(String source, String target, String relationshipName) {

    }

    @Override
    public StoreType getStoreType() {
        return StoreType.FILESYSTEM;
    }

    private void saveMetadata(Path path, Map<String, Object> metadata) throws IOException {

        metadata.put(StoragePropertyNames.ID.value(), String.valueOf(path));

        String propsFileName =  Files.isDirectory(path) ? "dir.properties" : path + ".properties";
        OutputStream output = new FileOutputStream(propsFileName);
        Properties prop = new Properties();
        metadata.forEach((k, v) -> {
                Object value;
                if (v == null) {
                    value = "";
                } else if (v instanceof List) {
                    value = ((List) v).stream().collect(Collectors.joining(","));
                } else
                    value = v;
                prop.put(k, value);
        });
        prop.store(output, null);
    }

    private Map<String, Object> getMetadata(String path) throws IOException {

        String propsFileName =  Files.isDirectory(Paths.get(path)) ? "dir.properties" : path + ".properties";
        InputStream input = new FileInputStream(propsFileName);
        Properties prop = new Properties();
        prop.load(input);
        return prop.entrySet().stream().collect(
                Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().toString()
                )
        );
    }
}
