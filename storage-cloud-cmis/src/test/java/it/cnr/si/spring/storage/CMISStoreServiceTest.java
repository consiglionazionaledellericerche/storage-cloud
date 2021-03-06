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

import it.cnr.si.spring.storage.bulk.StorageFile;
import it.cnr.si.spring.storage.config.StoragePropertyNames;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {AppConfig.class})
@TestPropertySource("classpath:application-test.properties")
public class CMISStoreServiceTest {

    public static final String TEXT = "hello worlds";
    public static final String TEXT2 = "hello worlds two";
    public static final String TITLE = "@''''^^^^^€";
    public static final String DESCRIPTION = "Raffaella Carrà";
    public static final String CM_TITLE = "cm:title";
    public static final String CM_DESCRIPTION = "cm:description";

    @Autowired
    private StoreService storeService;

    @Before
    public void testStore() throws IOException {
        InputStream is = IOUtils.toInputStream(TEXT, Charset.defaultCharset());
        Map<String, Object> map = new HashMap();
        map.put(StoragePropertyNames.NAME.value(), "test-file");
        map.put(StoragePropertyNames.OBJECT_TYPE_ID.value(), StoragePropertyNames.CMIS_DOCUMENT.value());
        map.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), Arrays.asList(StoragePropertyNames.ASPECT_TITLED.value()));
        map.put(CM_TITLE, TITLE);
        map.put(CM_DESCRIPTION, DESCRIPTION);
        StorageObject document = storeService.storeSimpleDocument(is, "text/plain", "/", map);
        InputStream iss = storeService.getResource(document.getKey());
        assertEquals(TEXT, IOUtils.toString(iss, Charset.defaultCharset()));
        assertEquals(TITLE, document.getPropertyValue(CM_TITLE));
        assertEquals(DESCRIPTION, document.getPropertyValue(CM_DESCRIPTION));

        final String folderPath = storeService.createFolderIfNotPresent(
                "/my-path",
                "my-name",
                "my-title",
                "my-description");
        assertNotNull(folderPath);
    }

    @After
    public void testGetAndDelete() throws IOException {
        InputStream is = storeService.getResource(storeService.getStorageObjectByPath("/test-file"));
        assertEquals(TEXT, IOUtils.toString(is, Charset.defaultCharset()));

        storeService.delete(storeService.getStorageObjectByPath("/test-file"));
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
        assertEquals(TEXT, IOUtils.toString(storeService.getResource(storageObject), Charset.defaultCharset()));
        assertEquals(BigInteger.valueOf(12), storageObject.<BigInteger>getPropertyValue(StoragePropertyNames.CONTENT_STREAM_LENGTH.value()));
        final StorageObject storageObject1 = storeService.getStorageObjectBykey(storeService.updateStream(
                storageObject.getKey(),
                IOUtils.toInputStream(TEXT2, Charset.defaultCharset()),
                MimeTypes.XML.mimetype()
        ).getKey());
        assertEquals(TEXT2, IOUtils.toString(storeService.getResource(storageObject1), Charset.defaultCharset()));
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

        Assert.assertEquals("1.0",
                Arrays.asList(storageObject.getKey().split(";"))
                        .stream()
                        .skip(1)
                        .findAny()
                        .orElse(null));

        StorageObject storageObject2 = storeService.restoreSimpleDocument(
                storageFile,
                new ByteArrayInputStream(storageFile.getBytes()),
                storageFile.getContentType(),
                storageFile.getFileName(),
                "/test",
                true);

        Assert.assertEquals("1.1",
                Arrays.asList(storageObject2.getKey().split(";"))
                        .stream()
                        .skip(1)
                        .findAny()
                        .orElse(null));

        StorageObject storageObject3 = storeService.restoreSimpleDocument(
                storageFile,
                new ByteArrayInputStream(storageFile.getBytes()),
                storageFile.getContentType(),
                storageFile.getFileName(),
                "/test",
                true);

        Assert.assertEquals("1.2",
                Arrays.asList(storageObject3.getKey().split(";"))
                        .stream()
                        .skip(1)
                        .findAny()
                        .orElse(null));

    }

}