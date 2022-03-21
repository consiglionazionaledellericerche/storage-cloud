package it.cnr.si.spring.storage;

import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@ContextConfiguration("classpath:storage-filesystem-test-context.xml")
@TestPropertySource("classpath:META-INF/spring/filesystem.properties")
public class GetChildrenTest {

    private final Logger log = LoggerFactory.getLogger(FilesystemStorageTest.class);

    @Autowired
    private StoreService storeService;
    @Autowired
    private StorageDriver storageDriver;

    private StorageObject savedFile;

    private static final String PIPPO = "pippo", PLUTO = "pluto", ASPECT = "aspect";


    @Before
    public void setUp() {
        InputStream is = new ByteArrayInputStream(PIPPO.getBytes());
        String contentType = "text/plain";
        String path = "/";
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(StoragePropertyNames.NAME.value(), PIPPO);
        savedFile = storeService.storeSimpleDocument(is, contentType, path, metadata);
        final String folderIfNotPresent = storeService.createFolderIfNotPresent(path, PLUTO, PLUTO, PLUTO);
        storeService.storeSimpleDocument(is, contentType, "/" + PLUTO, metadata);
    }

    @Test
    public void getChildrenDoesntReturnProperties() {

        List<StorageObject> children = storeService.getChildren("/");

        log.info("{}", children);

        Assert.isTrue(children.stream()
                        .map(so -> so.getPath())
                        .noneMatch(so -> so.endsWith("properties"))
        , "Some .properties files were found");

        Assert.isTrue(
                children.stream()
                        .filter(storageObject -> storageObject.getPropertyValue(StoragePropertyNames.NAME.value()).equals(PIPPO))
                        .findAny().isPresent(), "Object with name found"
        );
    }

    @Test
    public void getChildrenRecursiveDoesntReturnProperties() {

        List<StorageObject> children = storeService.getChildren("/", -1);

        log.info("{}", children);

        Assert.isTrue(children.stream()
                        .map(so -> so.getPath())
                        .noneMatch(so -> so.endsWith("properties"))
                , "Some .properties files were found");

        Assert.isTrue(
                children.stream()
                        .filter(storageObject -> storageObject.getPropertyValue(StoragePropertyNames.NAME.value()).equals(PIPPO))
                        .findAny().isPresent(), "Object with name found"
        );
        org.junit.Assert.assertEquals(
                children.stream()
                        .filter(storageObject -> storageObject.getPropertyValue(StoragePropertyNames.NAME.value()).equals(PIPPO))
                        .count(), 2
        );
    }

    @Test
    public void addAspect() {
        storeService.addAspect(savedFile, ASPECT);
        Assert.isTrue(storeService.getStorageObjectBykey(savedFile.getKey()).<List<String>>getPropertyValue(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value())
                .contains(ASPECT),"Aspect is present");
        Assert.isTrue(storeService.getStorageObjectBykey(savedFile.getKey()).<String>getPropertyValue(StoragePropertyNames.NAME.value())
                .equals(PIPPO),"Name is present");

    }

    @After
    public void after() {
        storeService
                .getChildren("/")
                .forEach(storageObject -> {
                    storeService.delete(storageObject);
                });
    }

}
