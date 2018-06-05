package it.cnr.si.spring.storage.bulk;

import it.cnr.si.spring.storage.StorageObject;

import java.io.Serializable;

public class StorageDocument implements Serializable {
    private final StorageObject storageObject;

    public StorageDocument(StorageObject storageObject) {
        super();
        this.storageObject = storageObject;
    }

    public static StorageDocument construct(StorageObject storageObject) {
        return new StorageDocument(storageObject);
    }

    public StorageObject getStorageObject() {
        return storageObject;
    }
}
