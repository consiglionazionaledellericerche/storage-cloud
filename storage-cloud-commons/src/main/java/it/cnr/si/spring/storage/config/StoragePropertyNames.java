package it.cnr.si.spring.storage.config;

/**
 * Created by mspasiano on 6/15/17.
 */
public enum StoragePropertyNames {
    SYS_ARCHIVED("P:sys:archived"),
    NAME("cmis:name"),
    TITLE("cm:title"),
    DESCRIPTION("cm:description"),
    AUTHOR("cm:author"),
    ASPECT_TITLED("P:cm:titled"),
    OBJECT_TYPE_ID("cmis:objectTypeId"),
    BASE_TYPE_ID("cmis:baseTypeId"),
    ID("cmis:objectId"),
    PATH("cmis:path"),
    CMIS_FOLDER("cmis:folder"),
    CMIS_DOCUMENT("cmis:document"),
    ALFCMIS_NODEREF("alfcmis:nodeRef"),
    CONTENT_STREAM_LENGTH("cmis:contentStreamLength"),
    CONTENT_STREAM_MIME_TYPE("cmis:contentStreamMimeType"),
    SECONDARY_OBJECT_TYPE_IDS("cmis:secondaryObjectTypeIds");

    private String value;

    StoragePropertyNames(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }
}
