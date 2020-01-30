package it.cnr.si.spring.storage.condition;

import it.cnr.si.spring.storage.StorageDriver;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class StorageDriverIsCmis implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String driverName = context.getEnvironment().getProperty("cnr.storage.driver", "none");
        return driverName.equalsIgnoreCase(StorageDriver.StoreType.CMIS.toString() );
    }
}
