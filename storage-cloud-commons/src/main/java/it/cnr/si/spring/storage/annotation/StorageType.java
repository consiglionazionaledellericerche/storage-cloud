/**
 *
 */
package it.cnr.si.spring.storage.annotation;

import java.lang.annotation.*;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author mspasiano
 */
@Documented
@Retention(RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface StorageType {
    public String name();

    public String[] parentName() default "";
}
