package it.cnr.si.spring.storage.config;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

/**
 * Created by mspasiano on 6/5/17.
 */
@Configuration
public class S3StorageConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageConfiguration.class);

    @Bean
    AWSCredentials basicAWSCredentials(S3StorageConfigurationProperties s3StorageConfigurationProperties) {
        return Optional.ofNullable(s3StorageConfigurationProperties.getAccessKey())
                .filter(s -> !s.isEmpty())
                .map(s -> new BasicAWSCredentials(s, s3StorageConfigurationProperties.getSecretKey()))
                .orElse(null);
    }

    @Bean
    AmazonS3 amazonS3(AWSCredentials awsCredentials, S3StorageConfigurationProperties s3StorageConfigurationProperties) {

        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);

        AwsClientBuilder.EndpointConfiguration endpoint =
                new AwsClientBuilder.EndpointConfiguration(s3StorageConfigurationProperties.getAuthUrl(),
                        s3StorageConfigurationProperties.getSigningRegion());

        return AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfig)
                .withEndpointConfiguration(endpoint)
                .withCredentials(Optional.ofNullable(awsCredentials).map(awsCredential -> new AWSStaticCredentialsProvider(awsCredential)).orElse(null))
                .build();
    }
}
