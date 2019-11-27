package it.cnr.si.spring.storage;

import it.cnr.si.spring.storage.bulk.StorageFile;
import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.awt.*;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static org.junit.Assert.*;
import static it.cnr.si.spring.storage.config.StoragePropertyNames.*;

@RunWith(SpringRunner.class)
@ContextConfiguration("/storage-filesystem-test-context.xml")
public class FilesystemStorageTest {

    private final Logger log = LoggerFactory.getLogger(FilesystemStorageTest.class);

    @Autowired
    private StoreService storeService;

    @Test
    public void testCreateAndReadDirectoryAndMetadata() {
        String TITOLO = "Titolo Uno";
        String DESCRIZIONE = "Descrizione Uno";
        String folder = storeService.createFolderIfNotPresent("/", "uno", TITOLO, DESCRIZIONE);

        StorageObject so = storeService.getStorageObjectBykey(folder);

        log.info("{}", so);

        assertNotNull(so);
        assertEquals( TITOLO, so.getPropertyValue(TITLE.value()) );
        assertEquals( DESCRIZIONE, so.getPropertyValue(DESCRIPTION.value()) );
        assertEquals( folder, so.getKey() );
        assertEquals( folder, so.getPath() );

        folder = storeService.createFolderIfNotPresent("", "due", TITOLO, DESCRIZIONE);
        so = storeService.getStorageObjectBykey(folder);

        log.info("{}", so);

        assertNotNull(so);
        assertEquals( TITOLO, so.getPropertyValue(TITLE.value()) );
        assertEquals( DESCRIZIONE, so.getPropertyValue(DESCRIPTION.value()) );
        assertEquals( folder, so.getKey() );
        assertEquals( folder, so.getPath() );

        try {
            folder = storeService.createFolderIfNotPresent(null, "due", TITOLO, DESCRIZIONE);
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof NullPointerException);
        }
    }


    @Test
    public void testCreateAndReadSimpleDocumentAndMetadata() {
        String pippo = "pippo";
        InputStream is = new ByteArrayInputStream(pippo.getBytes());
        String contentType = "text/plain";
        String path = "/";
        Map<String, Object> metadata = new HashMap<>();
        StorageObject so = storeService.storeSimpleDocument(is, contentType, path, metadata);

        StorageObject tre = storeService.getStorageObjectBykey(so.getKey());
        StorageObject quattro = storeService.getStorageObjectByPath(so.getPath());

        log.info("{}", so);
        log.info("{}", tre);
        log.info("{}", quattro);

        assertEquals(so.toString(), tre.toString());
        assertEquals(so.toString(), quattro.toString());
        assertEquals(so.getPath(), tre.getPath());
        assertEquals(so.getKey(), tre.getKey());

        InputStream content = storeService.getResource(so.getKey());
        String contentString = new Scanner(content).next();

        log.info("{}", contentString);

        assertEquals(pippo, contentString);

    }

    @Test
    public void testRestoreSimpleDocument() {
        String pippo = "pippo";
        InputStream is = new ByteArrayInputStream(pippo.getBytes());

        StorageFile file = new StorageFile(is,
                "text/plain",
                "titolo");

        StorageObject so = storeService.restoreSimpleDocument(
                file,
                new ByteArrayInputStream(file.getBytes()),
                "text/plain",
                "Titolo",
                "/",
                true);

        log.info("{}", so);
        StorageObject quattro = storeService.getStorageObjectByPath(so.getPath());

        log.info("{}", quattro);

        assertEquals(so.getPath(), quattro.getPath());
        assertEquals(so.getKey(), quattro.getKey());

        InputStream content = storeService.getResource(so.getKey());
        String contentString = new Scanner(content).next();

        log.info("{}", contentString);

        assertEquals(pippo, contentString);
    }
}
