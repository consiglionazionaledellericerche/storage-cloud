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
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by marco.spasiano on 13/07/17.
 */

@RunWith(SpringRunner.class)
@ContextConfiguration("/storage-s3-test-context.xml")
@TestPropertySource("classpath:META-INF/spring/s3.properties")
public class S3StoreServiceTest {

    public static final String TEXT = "hello worlds";

    @Autowired
    private StoreService storeService;

    @Test
    public void testStore() throws IOException {
        InputStream is = IOUtils.toInputStream(TEXT, Charset.defaultCharset());
        Map<String, Object> map = new HashMap();
        map.put(StoragePropertyNames.NAME.value(), "ciaone");
        map.put("email", "francesco@uliana.it");
        map.put("titolo", "�COLE POLYTECHNIQUE F�D�RALE DE LAUSANNE EPFL");
        map.put("name", "Raffaella Carrà");
        StorageObject document = storeService.storeSimpleDocument(is, "text/plain", "/foo", map);
        InputStream iss = storeService.getResource("foo/ciaone");
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
        InputStream is = storeService.getResource("/foo/ciaone");
        assertEquals(TEXT, IOUtils.toString(is, Charset.defaultCharset()));

        storeService.delete(storeService.getStorageObjectBykey("/foo/ciaone"));
        storeService.delete(storeService.getStorageObjectBykey("/my-path/my-name"));
    }

}