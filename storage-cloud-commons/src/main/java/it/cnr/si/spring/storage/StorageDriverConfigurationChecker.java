package it.cnr.si.spring.storage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import jakarta.annotation.PostConstruct;
import java.util.Arrays;

@Configuration
public class StorageDriverConfigurationChecker {

    @Autowired
    private Environment env;

    @PostConstruct
    public void init(){

        String driverName = env.getProperty("cnr.storage.driver");

        boolean noDriverNameSet = Arrays.asList(StorageDriver.StoreType.values()).stream()
                .map(String::valueOf)
                .noneMatch(type -> type.equalsIgnoreCase(driverName));

        if (noDriverNameSet)
            throw new IllegalArgumentException("At least one driverName among 'azure', 'cmis', 'filesystem' or 's3' must be set in property 'cnr.storage.driver'. Current value is: "+ driverName);

    }

}
