<h1 align="center">
  <a href="https://github.com/consiglionazionaledellericerche/storage-cloud">
    Object storage built to store and retrieve any amount of data from anywhere
  </a>
</h1>
<p align="center">
  Artifact that allows integration with different archives and is composed of the following modules:
</p>  
<ol>
    <li><a href="https://en.wikipedia.org/wiki/Content_Management_Interoperability_Services">CMIS</a></li>
    <li><a href="https://azure.microsoft.com/en-us/services/storage/">AZURE</a></li>
    <li><a href="https://aws.amazon.com/s3/">Amazon S3</a></li>
    <li>Local filesystem (for test and demo purposes)</li>
</ol>
<p align="center">
  <a href="https://github.com/consiglionazionaledellericerche/storage-cloud/blob/master/LICENSE">
    <img src="https://img.shields.io/badge/License-AGPL%20v3-blue.svg" alt="storage-cloud is released under the GNU AGPL v3 license." />
  </a>
  <a href="https://mvnrepository.com/artifact/it.cnr.si.storage/storage-cloud">
    <img alt="Maven Central" src="https://img.shields.io/maven-central/v/it.cnr.si.storage/storage-cloud.svg?style=flat" alt="Current version on maven central.">
  </a>
</p>

## MAVEN dependency
|Artifact| Version |
|---|---|
|[Apache Chemistry](https://chemistry.apache.org/java/opencmis.html)| ![Maven Central](https://img.shields.io/maven-central/v/org.apache.chemistry.opencmis/chemistry-opencmis-client-impl.svg)|
|[Spring.io](https://spring.io/)| ![Maven Central with version prefix filter](https://img.shields.io/maven-central/v/org.springframework/spring-context/5.1.8.RELEASE.svg) |
|[AZURE](https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage) | ![Maven Central](https://img.shields.io/maven-central/v/com.microsoft.azure/azure-storage/5.3.1.svg)|
|[AMAZON S3](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-s3) | ![Maven Central](https://img.shields.io/maven-central/v/com.amazonaws/aws-java-sdk-s3/1.11.84.svg)|

## Usage
```java
    InputStream is = IOUtils.toInputStream(TEXT, Charset.defaultCharset());
    Map<String, Object> map = new HashMap();
    map.put(StoragePropertyNames.NAME.value(), "test-file");
    map.put(StoragePropertyNames.OBJECT_TYPE_ID.value(), StoragePropertyNames.CMIS_DOCUMENT.value());
    map.put(StoragePropertyNames.SECONDARY_OBJECT_TYPE_IDS.value(), Arrays.asList(StoragePropertyNames.ASPECT_TITLED.value()));
    map.put(CM_TITLE, TITLE);
    map.put(CM_DESCRIPTION, DESCRIPTION);
    StorageObject document = storeService.storeSimpleDocument(is, "text/plain", "/", map);

    InputStream iss = storeService.getResource(document.getKey());
    assertEquals(TEXT, IOUtils.toString(iss, Charset.defaultCharset()));
    assertEquals(TITLE, document.getPropertyValue(CM_TITLE));
    assertEquals(DESCRIPTION, document.getPropertyValue(CM_DESCRIPTION));

    final String folderPath = storeService.createFolderIfNotPresent(
            "/my-path",
            "my-name",
            "my-title",
            "my-description");
    assertNotNull(folderPath);
```

## Configuration

All configuration properties are listed here. The properties' names are dependent on the chosen driver

```properties
cnr.storage.driver=

cnr.storage.azure.connectionString=
cnr.storage.azure.containerName=

cnr.storage.filesystem.directory=

cnr.storage.s3.authUrl=
cnr.storage.s3.accessKey=
cnr.storage.s3.secretKey=
cnr.storage.s3.bucketName=
cnr.storage.s3.deleteAfter=
cnr.storage.s3.signingRegion=
```

CMIS properties are an exception, as they are loaded by the [Chemistry](https://chemistry.apache.org/java/opencmis.html) library. The relevant properties are
```properties
repository.base.url=
org.apache.chemistry.opencmis.[...]=
``` 
See [SessionParameter](https://svn.apache.org/repos/asf/chemistry/opencmis/trunk/chemistry-opencmis-commons/chemistry-opencmis-commons-api/src/main/java/org/apache/chemistry/opencmis/commons/SessionParameter.java) for a reference of all OpenCmis parameters.

A minimal set of parameters is
```properties
repository.base.url=
org.apache.chemistry.opencmis.session.repository.id=
org.apache.chemistry.opencmis.binding.atompub.url=
org.apache.chemistry.opencmis.binding.browser.url=
org.apache.chemistry.opencmis.binding.spi.type=
org.apache.chemistry.opencmis.binding.connecttimeout=
org.apache.chemistry.opencmis.binding.readtimeout=
org.apache.chemistry.opencmis.binding.httpinvoker.classname=
org.apache.chemistry.opencmis.user=
org.apache.chemistry.opencmis.password=
```

## 👏 How to Contribute

The main purpose of this repository is to continue evolving storage-cloud. We want to make contributing to this project as easy and transparent as possible, and we are grateful to the community for contributing bugfixes and improvements.

## 📄 License

storage-cloud is GNU AFFERO GENERAL PUBLIC LICENSE licensed, as found in the [LICENSE][l] file.

[l]: https://github.com/consiglionazionaledellericerche/storage-cloud/blob/master/LICENSE