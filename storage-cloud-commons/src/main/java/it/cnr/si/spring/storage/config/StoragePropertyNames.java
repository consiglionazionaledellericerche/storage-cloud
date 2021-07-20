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
    SECONDARY_OBJECT_TYPE_IDS("cmis:secondaryObjectTypeIds"),
    LAST_MODIFIED("cmis:lastModificationDate");
    private String value;

    StoragePropertyNames(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }
}
