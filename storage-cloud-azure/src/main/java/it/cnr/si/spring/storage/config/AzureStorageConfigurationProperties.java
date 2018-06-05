package it.cnr.si.spring.storage.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Created by mspasiano on 6/5/17.
 */
@Configuration
public class AzureStorageConfigurationProperties {

    @Value("${cnr.azure.connectionString}")
    private String connectionString;

    @Value("${cnr.azure.containerName}")
    private String containerName;

    @Value("#{${store.metadataKeys}}")
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
