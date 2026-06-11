package com.example.downstream.ratelimit;

import com.example.kafka.Profiles;
import com.example.kafka.ratelimit.DisabledRateLimiter;
import com.example.kafka.ratelimit.RateLimiterWrapper;
import com.example.kafka.ratelimit.RateLimiterWrapperImpl;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Layer 1 — wiring/config test (no broker). Validates that the {@code kafka-rate-limit-enabled}
 * profile selects the right bean type for each of the three named limiters and the composite, and
 * that the {@code kafka.rate-limit.*} properties bind through to the Guava limiter's configured rate.
 * Does NOT re-test Guava's throttling behavior — that's covered by common-configs'
 * {@code RateLimiterWrapperImplTest}.
 */
class RateLimiterConfigWiringTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(RateLimiterConfig.class);

    @Test
    void profileOff_allThreeAreDisabledNoOps_andCompositeWiresThem() {
        runner.run(ctx -> {
            assertThat(ctx.getBean("bioRateLimiter", RateLimiterWrapper.class)).isInstanceOf(DisabledRateLimiter.class);
            assertThat(ctx.getBean("matchRateLimiter", RateLimiterWrapper.class)).isInstanceOf(DisabledRateLimiter.class);
            assertThat(ctx.getBean("socialRateLimiter", RateLimiterWrapper.class)).isInstanceOf(DisabledRateLimiter.class);

            DownstreamRateLimiters composite = ctx.getBean(DownstreamRateLimiters.class);
            assertThat(composite.bio()).isInstanceOf(DisabledRateLimiter.class);
            assertThat(composite.match()).isInstanceOf(DisabledRateLimiter.class);
            assertThat(composite.social()).isInstanceOf(DisabledRateLimiter.class);
        });
    }

    @Test
    void profileOn_allThreeAreGuavaBacked_withRatesFromProperties_andCompositeHoldsSameInstances() {
        runner.withInitializer(c -> c.getEnvironment().addActiveProfile(Profiles.KAFKA_RATE_LIMIT_ENABLED))
                .withPropertyValues(
                        "kafka.rate-limit.bio=11.0",
                        "kafka.rate-limit.match=22.0",
                        "kafka.rate-limit.social=33.0")
                .run(ctx -> {
                    RateLimiterWrapper bio = ctx.getBean("bioRateLimiter", RateLimiterWrapper.class);
                    RateLimiterWrapper match = ctx.getBean("matchRateLimiter", RateLimiterWrapper.class);
                    RateLimiterWrapper social = ctx.getBean("socialRateLimiter", RateLimiterWrapper.class);

                    assertThat(bio).isInstanceOf(RateLimiterWrapperImpl.class);
                    assertThat(match).isInstanceOf(RateLimiterWrapperImpl.class);
                    assertThat(social).isInstanceOf(RateLimiterWrapperImpl.class);
                    assertThat(bio.getRate()).isEqualTo(11.0);
                    assertThat(match.getRate()).isEqualTo(22.0);
                    assertThat(social.getRate()).isEqualTo(33.0);

                    DownstreamRateLimiters composite = ctx.getBean(DownstreamRateLimiters.class);
                    assertThat(composite.bio()).isSameAs(bio);
                    assertThat(composite.match()).isSameAs(match);
                    assertThat(composite.social()).isSameAs(social);
                });
    }

    @Test
    void profileOn_ratesFallBackToYmlDefaultsWhenUnset() {
        runner.withInitializer(c -> c.getEnvironment().addActiveProfile(Profiles.KAFKA_RATE_LIMIT_ENABLED))
                .run(ctx -> {
                    // RateLimiterConfig declares ${kafka.rate-limit.<svc>:10.0} defaults.
                    assertThat(ctx.getBean("bioRateLimiter", RateLimiterWrapper.class).getRate()).isEqualTo(10.0);
                    assertThat(ctx.getBean("matchRateLimiter", RateLimiterWrapper.class).getRate()).isEqualTo(10.0);
                    assertThat(ctx.getBean("socialRateLimiter", RateLimiterWrapper.class).getRate()).isEqualTo(10.0);
                });
    }
}
