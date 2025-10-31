package com.db.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
@RequiredArgsConstructor
public class TradeScheduler {

    private final TradeService tradeService;

    // Run every day at midnight
    @Scheduled(cron = "0 0 0 * * ?")
    public void checkExpiredTrades() {
        tradeService.markExpiredTrades();
    }
}
