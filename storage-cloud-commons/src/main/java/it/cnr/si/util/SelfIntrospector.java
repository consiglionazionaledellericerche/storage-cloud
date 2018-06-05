package it.cnr.si.util;

public interface SelfIntrospector {
    public abstract Object getPropertyValue(String s);

    public abstract void setPropertyValue(String s, Object obj);
}
