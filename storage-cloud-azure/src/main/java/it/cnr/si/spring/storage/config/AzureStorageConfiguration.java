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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import it.cnr.si.spring.storage.AzureStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

/**
 * Created by mspasiano on 6/5/17.
 */
@Configuration
public class AzureStorageConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureStorageConfiguration.class);


    @Bean
    public CloudStorageAccount cloudBlobContainer(AzureStorageConfigurationProperties azureStorageConfigurationProperties) {

        String connectionString = azureStorageConfigurationProperties.getConnectionString();
        LOGGER.info("connectionString = {}", connectionString);
        try {
            StorageCredentials credentials = StorageCredentials
                    .tryParseCredentials(connectionString);
            return new CloudStorageAccount(credentials);
        } catch (InvalidKeyException | StorageException | URISyntaxException e) {
            String msg = "cannot get reference to blob container ";
            throw new it.cnr.si.spring.storage.StorageException(it.cnr.si.spring.storage.StorageException.Type.GENERIC, new RuntimeException(msg, e));
        }
    }


    @Bean
    public AzureStorageService azureBlobStorageService(CloudStorageAccount cloudStorageAccount,
                                                       AzureStorageConfigurationProperties azureStorageConfigurationProperties) {
        String containerName = azureStorageConfigurationProperties.getContainerName();
        LOGGER.info("container = {}", containerName);
        try {
            CloudBlobContainer container = cloudStorageAccount.createCloudBlobClient().getContainerReference(containerName);
            container.createIfNotExists();
            return new AzureStorageService(container, azureStorageConfigurationProperties);
        } catch (StorageException | URISyntaxException e) {
            String msg = "cannot get reference to blob container " + containerName;
            throw new it.cnr.si.spring.storage.StorageException(it.cnr.si.spring.storage.StorageException.Type.GENERIC, new RuntimeException(msg, e));
        }
    }


}
