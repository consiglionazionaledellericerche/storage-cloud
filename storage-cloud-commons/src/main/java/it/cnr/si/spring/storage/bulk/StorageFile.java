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

package it.cnr.si.spring.storage.bulk;


import it.cnr.si.spring.storage.StorageObject;
import it.cnr.si.spring.storage.annotation.StoragePolicy;
import it.cnr.si.spring.storage.annotation.StorageProperty;
import it.cnr.si.spring.storage.annotation.StorageType;
import it.cnr.si.spring.storage.config.StoragePropertyNames;

import jakarta.activation.MimetypesFileTypeMap;
import java.io.*;

@StorageType(name = "cmis:document")
public class StorageFile implements Serializable {
    private static final long serialVersionUID = -1775673719677028944L;

    private String contentType;
    private String fileName;
    private byte[] bytes;

    private String description;
    private String title;
    private String author;

    private StorageObject storageObject;

    public StorageFile() {
        super();
    }

    /**
     * Constructs a StorageFile
     *
     * @param bytes        the File object on the server which holds the uploaded contents of the file
     * @param contentType  the content type of the file declared by the browser during upload
     * @param originalName the name of the file as declared by the users browser
     */
    public StorageFile(byte[] bytes, String contentType, String originalName) {
        this.bytes = bytes;
        this.contentType = contentType;
        this.fileName = originalName;
        this.storageObject = null;
    }

    public StorageFile(InputStream inputStream, String contentType, String originalName) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            try {
                int c;
                while ((c = inputStream.read()) != -1) {
                    baos.write(c);
                }
            } finally {
                inputStream.close();
            }
        } catch (IOException e) {
        }
        this.bytes = baos.toByteArray();
        this.contentType = contentType;
        this.fileName = originalName;
        this.storageObject = null;
    }

    public StorageFile(StorageObject storageObject) {
        this.storageObject = storageObject;
        this.contentType = storageObject.<String>getPropertyValue(StoragePropertyNames.CONTENT_STREAM_MIME_TYPE.value());
        this.fileName = storageObject.<String>getPropertyValue(StoragePropertyNames.NAME.value());
        this.author = storageObject.<String>getPropertyValue(StoragePropertyNames.AUTHOR.value());
        this.description = storageObject.<String>getPropertyValue(StoragePropertyNames.DESCRIPTION.value());
        this.title = storageObject.<String>getPropertyValue(StoragePropertyNames.TITLE.value());
    }

    public StorageFile(File file, String contentType, String originalName) throws IOException {
        this(new FileInputStream(file), contentType != null ? contentType : new MimetypesFileTypeMap().getContentType(file), originalName);
    }

    public StorageFile(File file, String originalName) throws IOException {
        this(new FileInputStream(file), new MimetypesFileTypeMap().getContentType(file), originalName);
    }

    public StorageObject getStorageObject() {
        return storageObject;
    }

    public void setStorageObject(StorageObject storageObject) {
        this.storageObject = storageObject;
    }

    /**
     * Returns the name of the file that the user selected and uploaded (this is not necessarily
     * the name that the underlying file is now stored on the server using).
     *
     * @return file name
     */
    @StorageProperty(name = "cmis:name")
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * Returns the content type of the file that the user selected and uploaded.
     *
     * @return content type
     */
    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    /**
     * Gets an input stream to read from the file uploaded
     *
     * @return array of byte
     */
    public byte[] getBytes() {
        return bytes;
    }

    @StoragePolicy(name = "P:cm:titled", property = @StorageProperty(name = "cm:description"))
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @StoragePolicy(name = "P:cm:titled", property = @StorageProperty(name = "cm:title"))
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @StoragePolicy(name = "P:cm:author", property = @StorageProperty(name = "cm:author"))
    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public InputStream getInputStream() throws IOException {
        if (getBytes() != null && getBytes().length > 0)
            return new ByteArrayInputStream(getBytes());
        return null;
    }

    public String getStorageParentPath() {
        return null;
    }

    public String getStorageAlternativeParentPath() {
        return null;
    }
}
