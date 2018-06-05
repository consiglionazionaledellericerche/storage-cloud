package it.cnr.si.spring.storage;

import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by marco.spasiano on 13/07/17.
 */

@RunWith(SpringRunner.class)
@ContextConfiguration("/storage-cmis-test-context.xml")
public class CMISStoreServiceTest {

    public static final String TEXT = "hello worlds";

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

}