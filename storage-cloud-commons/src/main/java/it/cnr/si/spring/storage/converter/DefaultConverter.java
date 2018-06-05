package it.cnr.si.spring.storage.converter;

import java.io.Serializable;

public class DefaultConverter implements Converter<Serializable, Object> {

    public Serializable convert(Object obj) {
        return (Serializable) obj;
    }
}
