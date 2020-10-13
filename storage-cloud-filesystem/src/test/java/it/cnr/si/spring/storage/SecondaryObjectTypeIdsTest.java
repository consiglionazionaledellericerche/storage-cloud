package it.cnr.si.spring.storage;

import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@ContextConfiguration("classpath:storage-filesystem-test-context.xml")
@TestPropertySource("classpath:META-INF/spring/filesystem.properties")
public class SecondaryObjectTypeIdsTest {

    public static final String ASPECT_1 = "ASPECT 1";
    public static final String ASPECT_2 = "ASPECT 2";
    public static final String ASPECT_3 = "ASPECT 3";

    private static final String PIPPO = "pippo";
    private final Logger log = LoggerFactory.getLogger(FilesystemStorageTest.class);
    @Autowired
    private StoreService storeService;
    @Autowired
    private StorageDriver storageDriver;
    private StorageObject savedFile;

    @Before
    public void setUp() {
        InputStream is = new ByteArrayInputStream(PIPPO.getBytes());
        String contentType = "text/plain";
        String path = "/";

        Map<String, Object> metadata = Stream.of(
                new AbstractMap.SimpleEntry<>(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), Arrays.asList(ASPECT_1, ASPECT_2))
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        savedFile = storeService.storeSimpleDocument(is, contentType, path, metadata);
    }

    @Test
    public void getChildrenDoesntReturnProperties() {

        final StorageObject storageObjectBykey = storeService.getStorageObjectBykey(savedFile.getKey());
        assertEquals(2, storageObjectBykey.<List>getPropertyValue(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value()).size());
        assertTrue(storeService.hasAspect(storageObjectBykey, ASPECT_1));
        assertTrue(storeService.hasAspect(storageObjectBykey, ASPECT_2));

        storeService.addAspect(storageObjectBykey, ASPECT_3);
        assertTrue(storeService.hasAspect(storeService.getStorageObjectBykey(storageObjectBykey.getKey()), ASPECT_3));


    }
}
