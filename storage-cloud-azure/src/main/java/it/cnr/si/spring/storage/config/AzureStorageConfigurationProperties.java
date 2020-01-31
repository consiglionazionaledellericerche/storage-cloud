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

package it.cnr.si.spring.storage.config;

import it.cnr.si.spring.storage.condition.StorageDriverIsAzure;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Created by mspasiano on 6/5/17.
 */
@Configuration
@Conditional(StorageDriverIsAzure.class)
public class AzureStorageConfigurationProperties {

    @Value("${cnr.storage.azure.connectionString}")
    private String connectionString;

    @Value("${cnr.storage.azure.containerName}")
    private String containerName;

    @Value("#{${cnr.storage.metadataKeys}}")
    private Map<String, String> metadataKeys;


    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public Map<String, String> getMetadataKeys() {
        return metadataKeys;
    }

    public void setMetadataKeys(Map<String, String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }
}
