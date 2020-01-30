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

import it.cnr.si.spring.storage.condition.StorageDriverIsS3;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Created by mspasiano on 6/5/17.
 */
@Configuration
@Conditional(StorageDriverIsS3.class)
public class S3StorageConfigurationProperties {

    @Value("${cnr.aws.authUrl}")
    private String authUrl;
    @Value("${cnr.aws.accessKey}")
    private String accessKey;
    @Value("${cnr.aws.secretKey}")
    private String secretKey;
    @Value("${cnr.aws.bucketName}")
    private String bucketName;
    @Value("${cnr.aws.deleteAfter}")
    private Integer deleteAfter;
    @Value("${cnr.aws.signingRegion}")
    private String signingRegion;

    @Value("#{${store.metadataKeys}}")
    private Map<String, String> metadataKeys;

    public String getAuthUrl() {
        return authUrl;
    }

    public void setAuthUrl(String authUrl) {
        this.authUrl = authUrl;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public Integer getDeleteAfter() {
        return deleteAfter;
    }

    public void setDeleteAfter(Integer deleteAfter) {
        this.deleteAfter = deleteAfter;
    }

    public String getSigningRegion() {
        return signingRegion;
    }

    public void setSigningRegion(String signingRegion) {
        this.signingRegion = signingRegion;
    }

    public Map<String, String> getMetadataKeys() {
        return metadataKeys;
    }

    public void setMetadataKeys(Map<String, String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }
}
