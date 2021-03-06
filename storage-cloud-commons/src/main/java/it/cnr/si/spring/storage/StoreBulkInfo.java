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

package it.cnr.si.spring.storage;

import it.cnr.si.spring.storage.annotation.StoragePolicy;
import it.cnr.si.spring.storage.annotation.StorageProperties;
import it.cnr.si.spring.storage.annotation.StorageProperty;
import it.cnr.si.spring.storage.annotation.StorageType;
import it.cnr.si.spring.storage.bulk.StorageTypeName;
import it.cnr.si.util.Introspector;
import org.springframework.stereotype.Service;

import java.beans.IntrospectionException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by mspasiano on 6/15/17.
 */
@Service
public class StoreBulkInfo {
    private Map<Class<?>, String> cacheObjType = new Hashtable<Class<?>, String>();
    private Map<Class<?>, Set<String>> cacheObjAspect = new Hashtable<Class<?>, Set<String>>();
    private Map<Class<?>, Map<String, PropertyValue>> cacheObjPropertyDef = new Hashtable<Class<?>, Map<String, PropertyValue>>();
    private Map<Class<?>, Map<String, PropertyValue>> cacheObjAspectPropertyDef = new Hashtable<Class<?>, Map<String, PropertyValue>>();

    private List<Class<?>> getClassHierarchy(Class<?> targetClass, boolean reverse, boolean includeSelf) {
        List<Class<?>> hierarchy = new ArrayList<Class<?>>();
        if (!includeSelf) {
            targetClass = targetClass.getSuperclass();
        }
        while (targetClass != null) {
            if (reverse) {
                hierarchy.add(targetClass);
            } else {
                hierarchy.add(0, targetClass);
            }
            targetClass = targetClass.getSuperclass();
        }
        return hierarchy;
    }

    public String getType(Serializable oggettoBulk) {
        if (!(oggettoBulk instanceof StorageTypeName) && cacheObjType.containsKey(oggettoBulk.getClass()))
            return cacheObjType.get(oggettoBulk.getClass());
        if (oggettoBulk instanceof StorageTypeName)
            cacheObjType.put(oggettoBulk.getClass(), ((StorageTypeName) oggettoBulk).getTypeName());
        else {
            StorageType storageType = oggettoBulk.getClass().getAnnotation(StorageType.class);
            if (storageType == null && !(oggettoBulk instanceof StorageTypeName))
                throw new RuntimeException("Type is missing!");
            cacheObjType.put(oggettoBulk.getClass(), storageType.name());
        }
        return cacheObjType.get(oggettoBulk.getClass());
    }

    public List<String> getAspect(Serializable oggettoBulk) throws StorageException {
        if (cacheObjAspect.containsKey(oggettoBulk.getClass()))
            return new ArrayList(cacheObjAspect.get(oggettoBulk.getClass()));
        Set<String> results = new HashSet<String>();
        List<Class<?>> classHierarchy = getClassHierarchy(oggettoBulk.getClass(), false, true);
        for (Class<?> class1 : classHierarchy) {
            List<Field> attributi = Arrays.asList(class1.getDeclaredFields());
            for (Field field : attributi) {
                if (field.isAnnotationPresent(StoragePolicy.class)) {
                    results.add(field.getAnnotation(StoragePolicy.class).name());
                }
            }
            List<Method> methods = Arrays.asList(class1.getMethods());
            for (Method method : methods) {
                if (method.isAnnotationPresent(StoragePolicy.class)) {
                    results.add(method.getAnnotation(StoragePolicy.class).name());
                }
            }
        }
        cacheObjAspect.put(oggettoBulk.getClass(), results);
        return new ArrayList(cacheObjAspect.get(oggettoBulk.getClass()));
    }

    public Map<String, Object> getPropertyValue(Serializable oggettoBulk) {
        return getPropertyDefinition(oggettoBulk).entrySet().stream()
                .collect(
                        HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue().getValue(oggettoBulk)), HashMap::putAll
                );
    }

    public Map<String, PropertyValue> getPropertyDefinition(Serializable oggettoBulk) {
        if (cacheObjPropertyDef.containsKey(oggettoBulk.getClass()))
            return cacheObjPropertyDef.get(oggettoBulk.getClass());
        Map<String, PropertyValue> results = new HashMap<String, PropertyValue>();
        List<Class<?>> classHierarchy = getClassHierarchy(oggettoBulk.getClass(), false, true);
        for (Class<?> class1 : classHierarchy) {
            List<Field> attributi = Arrays.asList(class1.getDeclaredFields());
            for (Field field : attributi) {
                if (field.isAnnotationPresent(StorageProperty.class)) {
                    results.put(field.getAnnotation(StorageProperty.class).name(), new PropertyValue(field, null));
                }
                if (field.isAnnotationPresent(StorageProperties.class)) {
                    List<StorageProperty> properties = Arrays.asList(field.getAnnotation(StorageProperties.class).property());
                    for (StorageProperty storageProperty : properties) {
                        results.put(storageProperty.name(), new PropertyValue(field, null));
                    }
                }
            }
            List<Method> methods = Arrays.asList(class1.getMethods());
            for (Method method : methods) {
                if (method.isAnnotationPresent(StorageProperty.class)) {
                    results.put(method.getAnnotation(StorageProperty.class).name(), new PropertyValue(null, method));
                }
                if (method.isAnnotationPresent(StorageProperties.class)) {
                    List<StorageProperty> properties = Arrays.asList(method.getAnnotation(StorageProperties.class).property());
                    for (StorageProperty storageProperty : properties) {
                        results.put(storageProperty.name(), new PropertyValue(null, method));
                    }
                }
            }
        }
        cacheObjPropertyDef.put(oggettoBulk.getClass(), results);
        return cacheObjPropertyDef.get(oggettoBulk.getClass());
    }

    public Map<String, Object> getAspectPropertyValue(Serializable oggettoBulk) {
        return getAspectPropertyDefinition(oggettoBulk).entrySet().stream()
                .collect(
                        HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue().getValue(oggettoBulk)), HashMap::putAll
                );

    }

    public Map<String, PropertyValue> getAspectPropertyDefinition(Serializable oggettoBulk) {
        if (cacheObjAspectPropertyDef.containsKey(oggettoBulk.getClass()))
            return cacheObjAspectPropertyDef.get(oggettoBulk.getClass());
        Map<String, PropertyValue> results = new HashMap<String, PropertyValue>();
        List<Class<?>> classHierarchy = getClassHierarchy(oggettoBulk.getClass(), false, true);
        for (Class<?> class1 : classHierarchy) {
            List<Field> attributi = Arrays.asList(class1.getDeclaredFields());
            for (Field field : attributi) {
                if (field.isAnnotationPresent(StoragePolicy.class)) {
                    List<StorageProperty> properties = Arrays.asList(field.getAnnotation(StoragePolicy.class).property());
                    for (StorageProperty storageProperty : properties) {
                        results.put(storageProperty.name(), new PropertyValue(field, null));
                    }
                }
            }
            List<Method> methods = Arrays.asList(class1.getMethods());
            for (Method method : methods) {
                if (method.isAnnotationPresent(StoragePolicy.class)) {
                    List<StorageProperty> properties = Arrays.asList(method.getAnnotation(StoragePolicy.class).property());
                    for (StorageProperty storageProperty : properties) {
                        results.put(storageProperty.name(), new PropertyValue(null, method));
                    }
                }
            }
        }
        cacheObjAspectPropertyDef.put(oggettoBulk.getClass(), results);
        return cacheObjAspectPropertyDef.get(oggettoBulk.getClass());
    }

    private class PropertyValue {
        private final Field field;
        private final Method method;

        public PropertyValue(Field field, Method method) {
            super();
            this.field = field;
            this.method = method;
        }

        public Object getValue(Serializable oggettoBulk) {
            return Optional.ofNullable(field)
                    .map(field1 -> {
                        try {
                            return Introspector.getPropertyValue(oggettoBulk, field1.getName());
                        } catch (IntrospectionException | InvocationTargetException e) {
                            throw new IllegalArgumentException(e);
                        }
                    }).orElse(
                            Optional.ofNullable(method)
                                    .map(method1 -> {
                                        try {
                                            return Introspector.invoke(oggettoBulk, method1);
                                        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                                            throw new IllegalArgumentException(e);
                                        }
                                    })
                                    .orElse(null)
                    );
        }

    }

}