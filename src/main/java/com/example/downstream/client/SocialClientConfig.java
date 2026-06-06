package com.example.downstream.client;

import com.example.social.SocialClient;
import com.example.social.SocialClientImpl;
import com.example.social.SocialConfig;
import com.example.social.api.SocialApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Profile-gated {@link SocialClient} bean wrapping the generated {@link SocialApi}. The lib's
 * {@code socialApi} bean stays available; declaring our own {@code SocialClient} suppresses the lib's
 * {@code @ConditionalOnMissingBean} default.
 */
@Configuration
@AutoConfigureBefore(SocialConfig.class)
@ConditionalOnProperty("app.social.base-url")
public class SocialClientConfig {

    @Autowired
    private SocialApi socialApi;

    @Bean
    @Profile("no-cb")
    public SocialClient socialClientPlain() {
        return new SocialClientImpl(socialApi);
    }

    @Bean
    @Profile("!no-cb")
    public SocialClient socialClientCircuitBreaker() {
        return new SocialClientCircuitBreaker(new SocialClientImpl(socialApi));
    }
}
