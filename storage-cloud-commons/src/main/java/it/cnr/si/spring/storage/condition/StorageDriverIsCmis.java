package it.cnr.si.spring.storage.condition;

import it.cnr.si.spring.storage.StorageDriver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.Optional;

public class StorageDriverIsCmis implements Condition {
    @Value("${cnr.storage.driver}")
    private String driverNameProperty;

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String driverName = context.getEnvironment().getProperty("cnr.storage.driver", Optional.ofNullable(driverNameProperty).orElse("none"));
        return driverName.equalsIgnoreCase(StorageDriver.StoreType.CMIS.toString() );
    }
}
