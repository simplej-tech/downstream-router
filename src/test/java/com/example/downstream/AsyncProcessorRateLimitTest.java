package com.example.downstream;

import com.example.bio.BioQueryClient;
import com.example.bio.ProcessResponse;
import com.example.downstream.model.RequestMessage;
import com.example.downstream.ratelimit.DownstreamRateLimiters;
import com.example.kafka.ratelimit.RateLimiterWrapper;
import com.example.match.MatchClient;
import com.example.match.MatchResponse;
import com.example.social.SocialClient;
import com.example.social.SocialResponse;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Layer 2 — flow-logic test (no broker, no timing). Proves the processor acquires exactly one permit
 * per downstream call, on the right limiter, in bio → match → social order, immediately before each
 * call. This is the part the lib's wrapper tests can't cover: that OUR code actually routes through
 * the limiter at the right points. Timing/throttling is asserted separately by the Layer 3 IT.
 */
class AsyncProcessorRateLimitTest {

    @Test
    void acquiresOnePermitPerDownstreamCall_inBioMatchSocialOrder() {
        BioQueryClient bio = mock(BioQueryClient.class);
        MatchClient match = mock(MatchClient.class);
        SocialClient social = mock(SocialClient.class);
        when(bio.process(any())).thenReturn(new ProcessResponse("id1", "ok"));
        when(match.match(any())).thenReturn(new MatchResponse("id1", "ok", 1.0));
        when(social.lookup(any())).thenReturn(new SocialResponse("id1", "ok", 1L));

        RateLimiterWrapper bioRl = mock(RateLimiterWrapper.class);
        RateLimiterWrapper matchRl = mock(RateLimiterWrapper.class);
        RateLimiterWrapper socialRl = mock(RateLimiterWrapper.class);
        DownstreamRateLimiters limiters = new DownstreamRateLimiters(bioRl, matchRl, socialRl);

        AsyncProcessor processor = new AsyncProcessor(bio, match, social, limiters);
        processor.processRecord(new RequestMessage("id1", "standard", "payload"));

        InOrder order = inOrder(bioRl, bio, matchRl, match, socialRl, social);
        order.verify(bioRl).acquire();
        order.verify(bio).process(any());
        order.verify(matchRl).acquire();
        order.verify(match).match(any());
        order.verify(socialRl).acquire();
        order.verify(social).lookup(any());
        order.verifyNoMoreInteractions();

        // Each limiter acquired exactly once for one record — no double-acquire, no missed call.
        verifyNoMoreInteractions(bioRl, matchRl, socialRl);
    }
}
