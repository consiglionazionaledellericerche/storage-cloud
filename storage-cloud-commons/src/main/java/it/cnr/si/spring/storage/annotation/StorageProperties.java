package it.cnr.si.spring.storage.annotation;

import java.lang.annotation.*;

@Documented
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@Inherited
public @interface StorageProperties {
    public StorageProperty[] property();
}
