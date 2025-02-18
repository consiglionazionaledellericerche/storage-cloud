package it.cnr.si.spring.storage;

import it.cnr.si.spring.storage.condition.StorageDriverIsFilesystem;
import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.PostConstruct;
import java.io.*;
import java.math.BigInteger;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by mtrycz on 08/11/2019
 */

@Service
@Conditional(StorageDriverIsFilesystem.class)
public class FilesystemStorageDriver implements StorageDriver {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemStorageDriver.class);

    @Value("${cnr.storage.filesystem.directory}")
    private String directory;

    private Path basePath;

    @Override
    @PostConstruct
    public void init() {

        LOGGER.warn("Filsystem driver is meant for testing only. Don't use in production.");

        try {
            this.basePath = Paths.get(directory);
            Files.createDirectories(basePath);
            saveMetadata(Paths.get("/"), new HashMap<>());

        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Init: unable to create base storage directory " + basePath, e);
        }
    }

    @Override
    public StorageObject createFolder(String path, String name, Map<String, Object> metadata) {
        String relativePathName = sanitizePathName(path, name);
        Path relativePath = Paths.get(relativePathName);
        Path absolutePath = absolutizePath(relativePath);
        try {
            Files.createDirectories(absolutePath);
            saveMetadata(relativePath, metadata);
            return new StorageObject(relativePath.toString(), relativePath.toString(), metadata);

        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Unable to create directory " + absolutePath, e);
        }
    }

    @Override
    public StorageObject createDocument(InputStream inputStream,
                                        String contentType,
                                        Map<String, Object> metadataProperties,
                                        StorageObject parentObject,
                                        String path,
                                        boolean makeVersionable,
                                        StorageDriver.Permission... permissions) {

        if (makeVersionable)
            LOGGER.debug("Filesystem storage is meant for testing only. Versionable logic is ignored");
        if (permissions.length > 0)
            LOGGER.debug("Filesystem storage is meant for testing only. Permission logic is ignored");

        metadataProperties.put(StoragePropertyNames.CONTENT_STREAM_MIME_TYPE.value(), contentType);

        String filename = Optional.ofNullable(metadataProperties.get(StoragePropertyNames.NAME.value()))
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .orElse(UUID.randomUUID().toString());
        Path relativePath = Paths.get(
                parentObject == null ? parentObject.getPath() : path,
                filename);

        try {
            metadataProperties.put(StoragePropertyNames.CONTENT_STREAM_LENGTH.value(),
                    Files.copy(inputStream, absolutizePath(relativePath)));
            inputStream.close();
            saveMetadata(relativePath, metadataProperties);
            return new StorageObject(relativePath.toString(), relativePath.toString(), metadataProperties);
        } catch (FileAlreadyExistsException e) {
            throw new StorageException(StorageException.Type.CONSTRAINT_VIOLATED, "Unable to create file " + relativePath, e);
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Unable to create file " + relativePath, e);
        }
    }

    private StorageObject renameDirectory( StorageObject storageObject,Map<String, Object> metadataProperties,boolean renameDirFromUpdateProperties){
        Path objectPath = absolutizePath(Paths.get(storageObject.getKey()));
        try{
            Files.move(objectPath, objectPath.resolveSibling(metadataProperties.get( StoragePropertyNames.NAME.value()).toString()));
            Path newKey=relativizePath(Paths.get(objectPath.getParent().toString().
                    concat(SUFFIX).
                    concat(metadataProperties.get( StoragePropertyNames.NAME.value()).toString())));
            StorageObject soDirectory= getObject(newKey.toString());
            List<StorageObject> listChildren = getChildren(soDirectory.getKey());
            if ( listChildren!=null && !listChildren.isEmpty()) {
                for ( StorageObject so: listChildren) {
                    if (!Files.isDirectory(absolutizePath(Paths.get(so.getKey())))) {
                        copyNode(so, soDirectory);
                    }else{
                        renameDirectory(so,getMetadata(Paths.get(so.getPath())),false);
                    }

                }
            }
            StorageObject so = getObject(newKey.toString());
            if ( !renameDirFromUpdateProperties )
                updateProperties(so, getMetadata(Paths.get(so.getPath())));
            return so;


        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Unable to rename the directory " + storageObject.getPath(), e);
        }
    }
    private StorageObject checkIsAndRenameDirectory(StorageObject storageObject,Map<String, Object> metadataProperties,boolean renameDirFromUpdateProperties)  {
        Path objectPath = absolutizePath(Paths.get(storageObject.getKey()));
        if (Files.isDirectory(objectPath)&&
        metadataProperties.containsKey(StoragePropertyNames.NAME.value())
                && (!metadataProperties.get(StoragePropertyNames.NAME.value()).toString().equals(storageObject.getPropertyValue(StoragePropertyNames.NAME.value())))) {
            //rename Directory
            return renameDirectory( storageObject,metadataProperties,renameDirFromUpdateProperties);
        }
        return storageObject;
    }
    private StorageObject updateDocument(StorageObject storageObject,String fileName) throws IOException {

            if (!storageObject.getPropertyValue(StoragePropertyNames.NAME.value()).equals(fileName)) {

                String currenFileName=storageObject.getPropertyValue(StoragePropertyNames.NAME.value());
                Path objectPath = absolutizePath(Paths.get(storageObject.getPath()));
                Path propsPath = Paths.get(absolutizePath(Paths.get(storageObject.getPath())) + ".properties");
                try {
                    Files.move(propsPath, propsPath.resolveSibling(storageObject.getPropertyValue( StoragePropertyNames.NAME.value()).toString().concat(".properties")));
                } catch (IOException e) {
                    throw new StorageException(StorageException.Type.GENERIC, "Unable to move metadata for file " + storageObject.getPath()+" File Rename", e);
                }
                try {
                    Files.move(objectPath, objectPath.resolveSibling(storageObject.getPropertyValue( StoragePropertyNames.NAME.value()).toString().toString()));
                } catch (IOException e) {
                    throw new StorageException(StorageException.Type.GENERIC, "Unable to move metadata for file " + storageObject.getPath()+" File Rename", e);
                }
                storageObject = getObject(relativizePath(Paths.get(objectPath.getParent().toString().concat(SUFFIX).concat(currenFileName))).toString());
            }
            updateProperties(storageObject,getMetadata(Paths.get(storageObject.getKey())));
            return storageObject;

    }
    @Override
    public void updateProperties(StorageObject storageObject, Map<String, Object> metadataProperties) {
        String path = checkIsAndRenameDirectory(storageObject,metadataProperties,true).getPath();
        try {
            saveMetadata(Paths.get(path), metadataProperties);
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Unable to update metadata for file " + path, e);
        }
    }

    @Override
    public StorageObject updateStream(String key, InputStream inputStream, String contentType) {

        Path relativePath = Paths.get(key);
        Path objectPath = absolutizePath(relativePath);
        if (!Files.exists(objectPath))
            throw new StorageException(StorageException.Type.NOT_FOUND, "Resource does not exist " + key);
        try {
            Map<String, Object> metadata = getMetadata(relativePath);
            metadata.put(StoragePropertyNames.CONTENT_STREAM_MIME_TYPE.value(), contentType);
            metadata.put(StoragePropertyNames.CONTENT_STREAM_LENGTH.value(),
                    Files.copy(inputStream, objectPath, StandardCopyOption.REPLACE_EXISTING));
            inputStream.close();
            saveMetadata(relativePath, metadata);
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Unable to update content for file " + key, e);
        }
        return getObject(key); // TODO ???
    }

    @Override
    public InputStream getInputStream(String name) {
        try {
            return Files.newInputStream(absolutizePath(Paths.get(name)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream getInputStream(String key, String versionId) {
        throw new NotImplementedException();
    }

    @Override
    public InputStream getInputStream(String key, Boolean majorVersion) {
        throw new NotImplementedException();
    }

    @Override
    public Boolean delete(String id) {
        try {
            Path objectPath = absolutizePath(Paths.get(id));
            if (Files.isDirectory(objectPath)) {
                Files.walk(objectPath)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);

            } else {
                Path parent = objectPath.getParent();
                Files.walk(parent)
                        .filter(path -> {
                            return path.getFileName().toString().startsWith(objectPath.getFileName().toString());
                        })
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
            return !Files.exists(objectPath);
        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, "Unable to delete " + id, e);
        }
    }

    @Override
    public StorageObject getObject(String key) {
        try {
            Path objectPath = absolutizePath(Paths.get(key));
            if (Files.exists(objectPath)) {
                Map<String, Object> metadata = getMetadata(Paths.get(key));
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
        try {
            Stream<Path> contents = Files.list(absolutizePath(Paths.get(key)));
            return contents
                    .filter(p -> !p.toString().endsWith(".properties"))
                    .map(p -> relativizePath(p))
                    .map(p -> getObject(p.toString()))
                    .collect(Collectors.toList());

        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }

    @Override
    public List<StorageObject> getChildren(String key, int depth) {
        try {
            Stream<Path> contents = Files.walk(absolutizePath(Paths.get(key)),
                    Optional.ofNullable(depth)
                            .map(integer -> integer == -1 ? Integer.MAX_VALUE : integer)
                            .orElse(depth)
            );
            return contents
                    .filter(path -> !Files.isDirectory(path))
                    .filter(p -> !p.toString().endsWith(".properties"))
                    .map(p -> relativizePath(p))
                    .map(p -> getObject(p.toString()))
                    .collect(Collectors.toList());

        } catch (IOException e) {
            throw new StorageException(StorageException.Type.GENERIC, e);
        }
    }

    @Override
    public List<StorageObject> search(String query) {
        LOGGER.warn("FILESYSTEM -> Not yet implemented");
        return Collections.emptyList();
    }

    @Override
    public String signDocuments(String json, String url) {
        LOGGER.warn("FILESYSTEM -> Not yet implemented -> signDocuments");
        return null;
    }

    @Override
    public void copyNode(StorageObject source, StorageObject target) {
        try {
            Path objectPathSource = absolutizePath(Paths.get(source.getKey()));
            Path objectPathTarget = absolutizePath(Paths.get(target.getKey()));
            if ( !Files.isDirectory(objectPathTarget) ) {
                throw new StorageException( StorageException.Type.GENERIC, "Init: unable to copyNode the target " + target.getPath()+ " isn't a Directory");
            }
            if ( Files.isDirectory(objectPathSource) ){
                throw new StorageException( StorageException.Type.GENERIC, "Init: unable to copyNode the source " + target.getPath() + " isn't a File");
            }
            Map<String, Object> metadataProperties = getMetadata(Paths.get(source.getPath()));

            File file= new File(absolutizePath(Paths.get(source.getPath())).toString());
            if ( objectPathSource.getParent().equals(objectPathTarget)){
                //update document
                updateDocument( source, file.getName());
                return;

            }
            InputStream inputStream = new FileInputStream(file);
            Files.copy(inputStream,
                    absolutizePath(Paths.get(target.getPath().concat(SUFFIX).concat(( String)metadataProperties.get(StoragePropertyNames.NAME.value())))),
                    StandardCopyOption.REPLACE_EXISTING);
            inputStream.close();
            saveMetadata(Paths.get(target.getPath().concat(SUFFIX).concat(file.getName())), metadataProperties);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void managePermission(StorageObject storageObject, Map<String, ACLType> permission, boolean remove) {
        LOGGER.warn("FILESYSTEM -> Not yet implemented");
    }

    @Override
    public void setInheritedPermission(StorageObject storageObject, Boolean inherited) {
        LOGGER.warn("FILESYSTEM -> Not yet implemented");
    }

    @Override
    public List<StorageObject> getRelationship(String key, String relationshipName, boolean fromTarget) {
        LOGGER.warn("FILESYSTEM -> Not yet implemented");
        return Collections.emptyList();
    }

    @Override
    public void createRelationship(String source, String target, String relationshipName) {
        LOGGER.warn("FILESYSTEM -> Not yet implemented");
    }

    @Override
    public StoreType getStoreType() {
        return StoreType.FILESYSTEM;
    }

    private void saveMetadata(Path relativePath, Map<String, Object> metadata) throws IOException {
        metadata.put(StoragePropertyNames.ID.value(), relativePath.toString());

        Path objectPath = absolutizePath(relativePath);
        Path propsPath = Files.isDirectory(objectPath) ?
                objectPath.resolve("dir.properties") :
                Paths.get(absolutizePath(relativePath) + ".properties");

        Properties prop = new Properties();
        Stream.concat(
                getMetadata(relativePath).entrySet().stream(),
                metadata.entrySet().stream()
        ).forEach(stringObjectEntry -> {
            Object value;
            if (stringObjectEntry.getValue() == null) {
                value = "";
            } else if (stringObjectEntry.getValue() instanceof List) {
                value = ((List) stringObjectEntry.getValue()).stream().collect(Collectors.joining(","));
                value = "[" + value + "]";
            } else {
                value = stringObjectEntry.getValue().toString();
            }
            prop.put(stringObjectEntry.getKey(), value);
        });
        OutputStream output = new FileOutputStream(propsPath.toString());
        prop.store(output, null);
        output.close();
    }

    private Map<String, Object> getMetadata(Path path) throws IOException {
        try {
            Path objectPath = absolutizePath(path);
            Path propsPath = Files.isDirectory(objectPath) ?
                    objectPath.resolve("dir.properties") :
                    Paths.get(objectPath + ".properties");
            InputStream input = new FileInputStream(propsPath.toString());
            Properties prop = new Properties();
            prop.load(input);
            input.close();
            Optional.ofNullable(prop.getProperty(StoragePropertyNames.CONTENT_STREAM_LENGTH.value())).ifPresent(o -> {
                prop.put(StoragePropertyNames.CONTENT_STREAM_LENGTH.value(), (new BigInteger(o)));
            });

            return prop.entrySet().stream()
                    .map(e -> {
                        String s = e.getValue().toString();
                        if (s.startsWith("[")) {
                            s = s.substring(1, s.length() - 1);
                            e.setValue(Arrays.asList(s.split(",")));
                        }
                        return e;
                    })
                    .collect(
                            Collectors.toMap(
                                    e -> e.getKey().toString(),
                                    e -> e.getValue()
                            )
                    );
        } catch (FileNotFoundException _ex) {
            return Collections.emptyMap();
        }
    }


    private String sanitizePathName(String path, String name) {
        String relativePathName = path + "/" + name;
        while (relativePathName.startsWith("/"))
            relativePathName = relativePathName.substring(1);
        return relativePathName;
    }

    /*
     * Per ogni file e directory voglio salvarli nel prefisso basePath
     * settato nelle properties
     * (es. /tmp/flows o C:\Users\User\AppData\Local\Temp\flows)
     * Per prefissare correttamente il path col basePath, prima ho bisogno
     * di "relativizzarlo"
     * (cioe' se il path parte dalla root (es "/" o "C:\")
     *  tolgo la root)
     */
    private Path absolutizePath(Path relative) {
        if (relative.getRoot() != null)
            relative = relative.getRoot().relativize(relative);
        Path resolved = basePath.resolve(relative);
        return resolved;
    }

    private Path relativizePath(Path absolutePath) {
        return basePath.relativize(absolutePath);
    }
}
