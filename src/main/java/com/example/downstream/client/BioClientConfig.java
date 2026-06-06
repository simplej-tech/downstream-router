package com.example.downstream.client;

import com.example.bio.BioQueryClient;
import com.example.bio.BioQueryClientImpl;
import com.example.bio.BioQueryConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Profile-gated {@link BioQueryClient} bean. Either profile registers exactly one bean, which
 * suppresses the lib's {@code @ConditionalOnMissingBean} default. The lib's {@code bioRestClient}
 * (ES low-level client) bean stays available for us to wrap.
 */
@Configuration
@AutoConfigureBefore(BioQueryConfig.class)
@ConditionalOnProperty("app.bio.host")
public class BioClientConfig {

    @Autowired
    private RestClient bioRestClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Bean
    @Profile("no-cb")
    public BioQueryClient bioQueryClientPlain() {
        return new BioQueryClientImpl(bioRestClient, objectMapper);
    }

    @Bean
    @Profile("!no-cb")
    public BioQueryClient bioQueryClientCircuitBreaker() {
        return new BioQueryClientCircuitBreaker(new BioQueryClientImpl(bioRestClient, objectMapper));
    }
}
