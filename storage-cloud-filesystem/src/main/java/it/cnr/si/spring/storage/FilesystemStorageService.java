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

import javax.annotation.PostConstruct;
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

    private Path basePath;

    @Override
    @PostConstruct
    public void init() {
        try {
            this.basePath = Paths.get(directory);
            Files.createDirectories(basePath);
            saveMetadata("/", new HashMap<>());

        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Init: unable to create base storage directory "+ basePath, e);
        }
    }

    @Override
    public StorageObject createFolder(String path, String name, Map<String, Object> metadata) {
        String relativePathName = path + name;
        Path absolutePath = preparePath(relativePathName);
        try {
            Files.createDirectories( absolutePath );
            saveMetadata(relativePathName, metadata);
            return new StorageObject(relativePathName, relativePathName, metadata);

        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Unable to create directory "+ absolutePath, e);
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

        metadataProperties.put("contentType", contentType);

        String filename = UUID.randomUUID().toString();
        Path relativePath = Paths.get(
                parentObject == null ? parentObject.getPath() : path,
                filename);

        try {

            Files.copy( inputStream, preparePath(relativePath) );
            inputStream.close();

            saveMetadata(relativePath.toString(), metadataProperties);
            return new StorageObject(relativePath.toString(), relativePath.toString(), metadataProperties);
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Unable to create file "+ relativePath, e);
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
            return Files.newInputStream( preparePath(name) );
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
    public StorageObject getObject(String key) {
        try {
            Path p = preparePath(key);
            if (Files.exists(p)) {
                Map<String, Object> metadata = getMetadata(key);
                StorageObject so = new StorageObject(key, key, metadata);

                return so;
            } else
                return null;
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }



    @Override
    public StorageObject getObject(String id, UsernamePasswordCredentials customCredentials) {
        throw new NotImplementedException();
    }

    @Override
    public StorageObject getObjectByPath(String path, boolean isFolder) {
        return getObject(path);
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

    private void saveMetadata(String path, Map<String, Object> metadata) throws IOException {
        metadata.put(StoragePropertyNames.ID.value(), String.valueOf(path));

        Path originalPath = preparePath(path);
        Path propsPath =  Files.isDirectory( originalPath ) ?
                originalPath.resolve("dir.properties") :
                Paths.get(preparePath(path) + ".properties");

        OutputStream output = new FileOutputStream(propsPath.toString());
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
        Path originalPath = preparePath(path);
        Path propsPath =  Files.isDirectory( originalPath ) ?
                originalPath.resolve("dir.properties") :
                Paths.get(preparePath(path) + ".properties");
        InputStream input = new FileInputStream(propsPath.toString());
        Properties prop = new Properties();
        prop.load(input);
        return prop.entrySet().stream().collect(
                Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().toString()
                )
        );
    }

    private Path preparePath(String relative) {
        relative = relative == null ? "" : relative;
        relative = relative.startsWith("/") ? relative.substring(1) : relative;
        return basePath.resolve(Paths.get(relative));
    }

    private Path preparePath(Path relative) {
        return preparePath(relative.toString());
    }
}
