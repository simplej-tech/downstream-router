package com.example.downstream.client;

import com.example.match.MatchClient;
import com.example.match.MatchClientImpl;
import com.example.match.MatchConfig;
import com.example.match.api.MatchApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Profile-gated {@link MatchClient} bean wrapping the generated {@link MatchApi}. The lib's
 * {@code matchApi} bean stays available; declaring our own {@code MatchClient} suppresses the lib's
 * {@code @ConditionalOnMissingBean} default.
 */
@Configuration
@AutoConfigureBefore(MatchConfig.class)
@ConditionalOnProperty("app.match.base-url")
public class MatchClientConfig {

    @Autowired
    private MatchApi matchApi;

    @Bean
    @Profile("no-cb")
    public MatchClient matchClientPlain() {
        return new MatchClientImpl(matchApi);
    }

    @Bean
    @Profile("!no-cb")
    public MatchClient matchClientCircuitBreaker() {
        return new MatchClientCircuitBreaker(new MatchClientImpl(matchApi));
    }
}
