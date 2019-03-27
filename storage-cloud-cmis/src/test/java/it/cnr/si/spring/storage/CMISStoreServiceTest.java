package it.cnr.si.spring.storage;

import it.cnr.si.spring.storage.bulk.StorageFile;
import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by marco.spasiano on 13/07/17.
 */

@RunWith(SpringRunner.class)
@ContextConfiguration("/storage-cmis-test-context.xml")
public class CMISStoreServiceTest {

    public static final String TEXT = "hello worlds";
    public static final String TEXT2 = "hello worlds two";


    @Autowired
    private StoreService storeService;

    @Test
    public void testStore() throws IOException {
        InputStream is = IOUtils.toInputStream(TEXT, Charset.defaultCharset());
        Map<String, Object> map = new HashMap();
        map.put(StoragePropertyNames.NAME.value(), "ciaone");
        map.put(StoragePropertyNames.OBJECT_TYPE_ID.value(), StoragePropertyNames.CMIS_DOCUMENT.value());
        map.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), Arrays.asList(StoragePropertyNames.ASPECT_TITLED.value()));
        map.put("cm:title", "@''''^^^^^");
//        map.put("titolo", "�COLE POLYTECHNIQUE F�D�RALE DE LAUSANNE EPFL");
//        map.put("name", "Raffaella Carr�");
        StorageObject document = storeService.storeSimpleDocument(is, "text/plain", "/", map);
        InputStream iss = storeService.getResource(document.getKey());
        assertEquals(TEXT, IOUtils.toString(iss, Charset.defaultCharset()));

        final String folderPath = storeService.createFolderIfNotPresent(
                "/my-path",
                "my-name",
                "my-title",
                "my-description");
        assertNotNull(folderPath);
    }

    @Test
    public void testGetAndDelete() throws IOException {
        InputStream is = storeService.getResource(storeService.getStorageObjectByPath("/ciaone"));
        assertEquals(TEXT, IOUtils.toString(is, Charset.defaultCharset()));

        storeService.delete(storeService.getStorageObjectByPath("/ciaone"));
        storeService.delete(storeService.getStorageObjectByPath("/my-path/my-name"));
    }

    @Test
    public void testVersionable() throws IOException {
        Optional.ofNullable(storeService.getStorageObjectByPath("/test/test.xml"))
                .ifPresent(storageObject -> storeService.delete(storageObject));
        StorageFile storageFile = new StorageFile(IOUtils.toInputStream(TEXT, Charset.defaultCharset()),
                MimeTypes.XML.mimetype(),
                "test.xml");
        final StorageObject storageObject = storeService.restoreSimpleDocument(
                storageFile,
                new ByteArrayInputStream(storageFile.getBytes()),
                storageFile.getContentType(),
                storageFile.getFileName(),
                "/test",
                true);
        assertEquals(TEXT, IOUtils.toString(storeService.getResource(storageObject),Charset.defaultCharset()));
        assertEquals(BigInteger.valueOf(12), storageObject.<BigInteger>getPropertyValue(StoragePropertyNames.CONTENT_STREAM_LENGTH.value()));
        final StorageObject storageObject1 = storeService.getStorageObjectBykey(storeService.updateStream(
                storageObject.getKey(),
                IOUtils.toInputStream(TEXT2, Charset.defaultCharset()),
                MimeTypes.XML.mimetype()
        ).getKey());
        assertEquals(TEXT2, IOUtils.toString(storeService.getResource(storageObject1),Charset.defaultCharset()));
        assertEquals(BigInteger.valueOf(16), storageObject1.<BigInteger>getPropertyValue(StoragePropertyNames.CONTENT_STREAM_LENGTH.value()));
    }

    @Test
    public void testGetKeyFromRestoreDocument() {

        Optional.ofNullable(storeService.getStorageObjectByPath("/test/test.xml"))
                .ifPresent(storageObject -> storeService.delete(storageObject));

        StorageFile storageFile = new StorageFile(IOUtils.toInputStream(TEXT, Charset.defaultCharset()),
                MimeTypes.XML.mimetype(),
                "test.xml");

        StorageObject storageObject = storeService.restoreSimpleDocument(
                storageFile,
                new ByteArrayInputStream(storageFile.getBytes()),
                storageFile.getContentType(),
                storageFile.getFileName(),
                "/test",
                true);

        StorageObject updatedSo = storeService.getStorageObjectBykey(
                storageObject.<String>getPropertyValue("cmis:objectId").split(";")[0]);

        System.out.println(storageObject.getKey());
        System.out.println(updatedSo.getKey());

        StorageObject storageObject2 = storeService.restoreSimpleDocument(
                storageFile,
                new ByteArrayInputStream(storageFile.getBytes()),
                storageFile.getContentType(),
                storageFile.getFileName(),
                "/test",
                true);

        StorageObject updatedSo2 = storeService.getStorageObjectBykey(
                storageObject.<String>getPropertyValue("cmis:objectId").split(";")[0]);

        System.out.println(storageObject2.getKey());
        System.out.println(updatedSo2.getKey());

        StorageObject storageObject3 = storeService.restoreSimpleDocument(
                storageFile,
                new ByteArrayInputStream(storageFile.getBytes()),
                storageFile.getContentType(),
                storageFile.getFileName(),
                "/test",
                true);

        StorageObject updatedSo3 = storeService.getStorageObjectBykey(
                storageObject.<String>getPropertyValue("cmis:objectId").split(";")[0]);

        System.out.println(storageObject3.getKey());
        System.out.println(updatedSo3.getKey());    }
}