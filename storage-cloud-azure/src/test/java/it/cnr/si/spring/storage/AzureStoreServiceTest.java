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

import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by marco.spasiano on 13/07/17.
 */

@RunWith(SpringRunner.class)
@ContextConfiguration("/storage-azure-test-context.xml")
@TestPropertySource("classpath:META-INF/spring/azure.properties")
public class AzureStoreServiceTest {

    public static final String TEXT = "hello worlds";
    public static final String TEXT_UPLOAD = "Upload hello worlds";

    public static final String FOO = StorageDriver.SUFFIX + "foo spazio";
    public static final String CIAONE = "ciaone";
    public static final String FOO_CIAONE = FOO + StorageDriver.SUFFIX + CIAONE;

    public static final String P_CM_TITLED = "P:cm:titled";
    public static final String TEST_PDF = "test.pdf";

    @Autowired
    private StoreService storeService;

    @Test
    public void testStore() throws IOException {
        InputStream is = IOUtils.toInputStream(TEXT, Charset.defaultCharset());
        Map<String, Object> map = new HashMap();
        map.put(StoragePropertyNames.NAME.value(), CIAONE);
        map.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), Arrays.asList(P_CM_TITLED));
        map.put(StoragePropertyNames.TITLE.value(), "Raffaella Carrà");
        StorageObject document = storeService.storeSimpleDocument(is, "text/plain", FOO, map);
        InputStream iss = storeService.getResource(FOO_CIAONE);
        assertEquals(TEXT, IOUtils.toString(iss, Charset.defaultCharset()));

        Map<String, Object> mapPdf = new HashMap();
        mapPdf.put(StoragePropertyNames.NAME.value(), TEST_PDF);
        mapPdf.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), Arrays.asList(P_CM_TITLED));

        final String folderPath = storeService.createFolderIfNotPresent(
                "/my-path",
                "my-name",
                "my-title",
                "my-description");
        assertNotNull(folderPath);

        storeService.storeSimpleDocument(this.getClass().getResourceAsStream("/" + TEST_PDF), MimeTypes.PDF.mimetype(), "/my-path/my-name", mapPdf);

    }

    @Test
    public void testGetAndDelete() throws IOException {
        final StorageObject storageObjectByPath = storeService.getStorageObjectByPath(FOO_CIAONE);
        assertEquals(Arrays.asList(P_CM_TITLED),
                storageObjectByPath.<List<String>>getPropertyValue(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value()));

        InputStream is = storeService.getResource(FOO_CIAONE);
        assertEquals(TEXT, IOUtils.toString(is, Charset.defaultCharset()));

        assertEquals(storeService.getStorageObjectBykey("/my-path/my-name/test.pdf").getKey(),
                storeService.getChildren("/my-path/my-name").stream()
                        .filter(storageObject -> storageObject.getKey().equals("my-path/my-name/test.pdf"))
                        .findFirst()
                        .get().getKey());

        storeService.delete(storeService.getStorageObjectBykey(FOO_CIAONE));
        storeService.delete(storeService.getStorageObjectBykey("/my-path/my-name"));
        storeService.delete(storeService.getStorageObjectBykey("/my-path/my-name/test.pdf"));
    }

    @Test
    public void testCopyNode() throws IOException {
       //final StorageObject storageObjectByPath = storeService.getStorageObjectByPath(FOO_CIAONE);
        final StorageObject storageObjectByPath = storeService.getStorageObjectByPath("Comunicazioni da ISS/000.000/Contratti/2023/Contratto 2023D000000002");
        final StorageObject storageObjectByPathDir = storeService.getStorageObjectByPath("Comunicazioni da ISS / 000.000 / Contratti / 2023 / Contratto 2023DTEST / ");
        storeService.copyNode(storageObjectByPath,storageObjectByPathDir);
        Map<String,Object> m= new HashMap<>();
        m.put( "test","test");
        storeService.updateProperties(m,storageObjectByPath);

    }
    @Test
    public void testUploadStream() throws IOException {
        InputStream is = IOUtils.toInputStream(TEXT, Charset.defaultCharset());
        final StorageObject storageObjectByPathDir = storeService.getStorageObjectByPath(FOO_CIAONE);

        storeService.updateStream(storageObjectByPathDir.getKey(),is,"text/plain");

    }
    @Test
    public void testMetadataOnDirectory() throws IOException {

        final StorageObject storageObjectByPathDir = storeService.getStorageObjectByPath("/my-path/my-name");

        Map<String,Object> m= new HashMap<>();
        m.put( "test","test");
        storeService.updateProperties(m,storageObjectByPathDir);

    }
    private void createDirectoryTest() throws IOException {
        InputStream is = IOUtils.toInputStream(TEXT, Charset.defaultCharset());
        Map<String, Object> map = new HashMap();
        map.put(StoragePropertyNames.NAME.value(), CIAONE);
        map.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), Arrays.asList(P_CM_TITLED));
        map.put(StoragePropertyNames.TITLE.value(), "Raffaella Carrà");
        StorageObject document = storeService.storeSimpleDocument(is, "text/plain", FOO, map);
        InputStream iss = storeService.getResource(FOO_CIAONE);
        assertEquals(TEXT, IOUtils.toString(iss, Charset.defaultCharset()));

        Map<String, Object> mapPdf = new HashMap();
        mapPdf.put(StoragePropertyNames.NAME.value(), TEST_PDF);
        mapPdf.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), Arrays.asList(P_CM_TITLED));
        storeService.storeSimpleDocument(this.getClass().getResourceAsStream("/" + TEST_PDF), MimeTypes.PDF.mimetype(), FOO+"/subdir", mapPdf);

    }

    @Test
    public void renameDirectory() throws IOException {

        final StorageObject storageObjectByPathDir = storeService.getStorageObjectByPath("Comunicazioni da ISS/000.000/Contratti/2023/Contratto 2023P000000012",true,false);
        Map<String,Object> m= new HashMap<>();
        m.put(StoragePropertyNames.NAME.value(), "Contratto 2023D000000012");
        storeService.updateProperties(m,storageObjectByPathDir);
       // InputStream iss = storeService.getResource(FOO+"/subdir2/"+ TEST_PDF);
       // assertNotNull(iss);

    }

    @Test
    public void testMetadataNameOnDirectory() throws IOException {
        createDirectoryTest();
        storeService.delete(FOO+"/subdir2/"+ TEST_PDF);
        final StorageObject storageObjectByPathDir = storeService.getStorageObjectByPath(FOO+"/subdir",true,false);
        Map<String,Object> m= new HashMap<>();
        m.put( "test","test");
        m.put(StoragePropertyNames.NAME.value(), "subdir2");
        storeService.updateProperties(m,storageObjectByPathDir);
        InputStream iss = storeService.getResource(FOO+"/subdir2/"+ TEST_PDF);
        assertNotNull(iss);

    }
    @Test
    public void testGetObjectDirectoryWithSubDir() throws IOException{
        String path="my-path/rename-dir/";
        final StorageObject storageObjectByPathDir = storeService.getStorageObjectByPath(path,true,false);
        assertEquals(path,storageObjectByPathDir.getPath());

    }
    @Test
    public void testGetObjectPath() throws IOException{
        String folder ="foo spazio/subdir5";
        final StorageObject storageObjectByPathDir = storeService.getStorageObjectByPath(folder);
        List<StorageObject> childrens = Optional.ofNullable(storeService.getStorageObjectByPath( folder,true,false)).map(so->storeService.getChildren(so.getKey())).orElse(Collections.emptyList());
        assertEquals("foo spazio",storageObjectByPathDir.getPath());

    }

    @Test
    public void testGetChildren() throws IOException {
        List<StorageObject> l =storeService.getChildren("/Comunicazioni a ISS/Missioni/000.001/Rimborso Missione/Anno 2024/000000001");
        assertNotNull(l);
    }

}