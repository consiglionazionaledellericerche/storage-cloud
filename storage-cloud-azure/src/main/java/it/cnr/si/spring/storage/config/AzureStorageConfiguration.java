package it.cnr.si.spring.storage.config;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import it.cnr.si.spring.storage.AzureStorageDriver;
import it.cnr.si.spring.storage.condition.StorageDriverIsAzure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Created by mspasiano on 6/5/17.
 */
@Configuration
@Conditional({StorageDriverIsAzure.class})
public class AzureStorageConfiguration {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(AzureStorageConfiguration.class);

    @Bean
    public BlobServiceClient blobServiceClient(
            AzureStorageConfigurationProperties azureStorageConfigurationProperties) {

        String connectionString =
                azureStorageConfigurationProperties.getConnectionString();

        try {
            return new BlobServiceClientBuilder()
                    .connectionString(connectionString)
                    .buildClient();

        } catch (RuntimeException e) {
            String msg =
                    "Cannot create Azure BlobServiceClient";

            throw new it.cnr.si.spring.storage.StorageException(
                    it.cnr.si.spring.storage.StorageException.Type.GENERIC,
                    new RuntimeException(msg, e)
            );
        }
    }

    @Bean
    public BlobContainerClient blobContainerClient(
            BlobServiceClient blobServiceClient,
            AzureStorageConfigurationProperties azureStorageConfigurationProperties) {

        String containerName =
                azureStorageConfigurationProperties.getContainerName();

        LOGGER.info("container = {}", containerName);

        try {
            BlobContainerClient containerClient =
                    blobServiceClient.getBlobContainerClient(containerName);

            containerClient.createIfNotExists();

            return containerClient;

        } catch (BlobStorageException e) {
            String msg =
                    "Cannot get reference to blob container "
                            + containerName;

            throw new it.cnr.si.spring.storage.StorageException(
                    it.cnr.si.spring.storage.StorageException.Type.GENERIC,
                    new RuntimeException(msg, e)
            );
        }
    }

    @Bean
    public AzureStorageDriver azureBlobStorageService(
            BlobContainerClient blobContainerClient,
            AzureStorageConfigurationProperties azureStorageConfigurationProperties) {

        return new AzureStorageDriver(
                blobContainerClient,
                azureStorageConfigurationProperties
        );
    }
}