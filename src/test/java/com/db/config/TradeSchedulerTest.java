package com.db.config;

import com.db.service.TradeScheduler;
import com.db.service.TradeService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TradeSchedulerTest {
    @Mock
    private TradeService tradeService;
    @InjectMocks
    private TradeScheduler tradeScheduler;

    @Test
    void shouldCallMarkExpiredTradesOnSchedule() {
        tradeScheduler.checkExpiredTrades();
        verify(tradeService).markExpiredTrades();
    }
}
