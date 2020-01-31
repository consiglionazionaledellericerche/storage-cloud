package it.cnr.si.spring.storage;

import it.cnr.si.spring.storage.annotation.StorageType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
