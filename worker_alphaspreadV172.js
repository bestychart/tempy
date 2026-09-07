// [AlphaSpread Worker] V172
// Data-plane worker for token loading, kline/indicator calculations, aggregate trades,
// the dedicated order-book profile runtime, and worker diagnostics.

"use strict";

const WORKER_VERSION = "V172";

const MARKET = Object.freeze({
    SPOT: "spot",
    FUTURES: "futures",
    ALPHA: "alpha"
});
const MARKET_VALUES = Object.freeze(Object.values(MARKET));
const DEFAULT_MARKET = MARKET.ALPHA;

const FUTURES_WEBSOCKET_ROUTE = Object.freeze({
    PUBLIC: "public",
    MARKET: "market"
});
const FUTURES_WEBSOCKET_ROUTE_VALUES = Object.freeze(Object.values(FUTURES_WEBSOCKET_ROUTE));
const MARKET_LABELS = Object.freeze({
    [MARKET.SPOT]: "Spot",
    [MARKET.FUTURES]: "Future",
    [MARKET.ALPHA]: "Alpha"
});

const MARKET_CONFIG = Object.freeze({
    [MARKET.ALPHA]: Object.freeze({
        tokenListUrl: "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list",
        klinesUrl: "https://www.binance.com/bapi/defi/v1/public/alpha-trade/klines",
        depthUrl: "https://www.binance.com/bapi/defi/v1/public/alpha-trade/fullDepth",
        websocketUrl: "wss://nbstream.binance.com/w3w/wsa/stream",
        websocketMode: "subscribe",
        depthLimit: 500,
        allowedDepthIntervals: Object.freeze(["100ms", "250ms", "500ms"]),
        depthIntervalDefault: "100ms"
    }),
    [MARKET.SPOT]: Object.freeze({
        exchangeInfoUrl: "https://api.binance.com/api/v3/exchangeInfo",
        klinesUrl: "https://api.binance.com/api/v3/klines",
        depthUrl: "https://api.binance.com/api/v3/depth",
        websocketUrl: "wss://stream.binance.com:9443/ws",
        websocketMode: "direct",
        depthLimit: 1000,
        allowedDepthIntervals: Object.freeze(["100ms", "1000ms"]),
        depthIntervalDefault: "100ms"
    }),
    [MARKET.FUTURES]: Object.freeze({
        exchangeInfoUrl: "https://fapi.binance.com/fapi/v1/exchangeInfo",
        klinesUrl: "https://fapi.binance.com/fapi/v1/klines",
        aggTradesUrl: "https://fapi.binance.com/fapi/v1/aggTrades",
        depthUrl: "https://fapi.binance.com/fapi/v1/depth",
        websocketUrl: "wss://fstream.binance.com",
        websocketMode: "direct",
        depthLimit: 1000,
        allowedDepthIntervals: Object.freeze(["100ms", "250ms", "500ms"]),
        depthIntervalDefault: "100ms"
    })
});

const CONFIG = Object.freeze({
    newCoinMs: 8 * 7 * 24 * 60 * 60 * 1000,
    midCoinMs: 365 * 24 * 60 * 60 * 1000,
    reconnectBaseMs: 900,
    reconnectMaxMs: 30000,
    snapshotRetryMax: 3,
    quoteRecoveryAnchorMaxAgeMs: 4000,
    diagnosticsErrorRepeatMs: 10000,
    diagnosticsMaxPayloadChars: 900,
    coldDiagnosticsMinIntervalMs: 1000,
    tokenCacheTtlMs: 5 * 60 * 1000,
    futuresOneSecondBootstrapTradeLimit: 1000,
    advancedTradeThresholdWindowMs: 5 * 60 * 1000,
    advancedTradeThresholdProgressIntervalMs: 1000,
    advancedTradeThresholdAllReservoirCapacity: 262144,
    advancedSignificantTradeHistoryCapacity: 4096,
    advancedTradeThresholdMinAllSamples: 200,
    advancedTradeThresholdMinAllPercentileSamples: 500,
    advancedTradeThresholdMinAllClusterSamples: 12,
    advancedTradeThresholdMinClusterFraction: 0.001,
    advancedTradeThresholdUsdWeightCapMultiplier: 10.0,
    advancedTradeThresholdClusterMinSeparation: 1.35,
    advancedTradeThresholdClusterMinVarianceReduction: 0.20,
    bigBuySignalDatabaseCleanupIntervalMs: 5 * 60 * 1000,
    bigBuySignalDatabaseRetryDelayMs: 60 * 1000,
    tursoOutboxFlushIntervalMs: 60 * 1000,
    tursoHttpTimeoutMs: 7000,
    tursoHttpMaximumAttempts: 4,
    tursoRetryBaseDelayMs: 250,
    tursoMaximumPullRows: 40000
});

const BIG_BUY_SIGNAL_DATABASE = Object.freeze({
    // Legacy database identifiers are retained for backward compatibility; V145+ stores Significant Trade signals.
    name: "alpha_spread_indicator_history",
    version: 4,
    storeName: "big_buy_signals",
    marketSymbolTimeIndex: "by_market_symbol_time",
    timeIndex: "by_time",
    buySignalType: "big_buy",
    sellSignalType: "big_sell",
    timeframeSeconds: 60,
    maxAgeCandles: 1998,
    retentionDays: 30,
    retentionSeconds: 30 * 24 * 60 * 60,
    maximumRecordsPerLoad: 4000,
    outboxStoreName: "turso_signal_outbox",
    outboxQueuedAtIndex: "by_queued_at"
});

const TURSO_SIGNAL_SYNC = Object.freeze({
    signalTableName: "alpha_significant_trade_signals",
    metaTableName: "alpha_signal_sync_meta",
    schemaVersion: 1,
    outboxBatchSize: 50,
    maximumOutboxBatchesPerFlush: 10,
    pullPageSize: 500
});

const TURSO_THRESHOLD_FALLBACK = Object.freeze({
    tableName: "turbo_thresholds",
    maximumAgeMs: 2 * 60 * 60 * 1000,
    // Deliberately 5m + 2s rather than exactly 5m. The Python producer evaluates
    // on a 5-minute cadence, so the browser's read-only SELECT drifts away from it.
    refreshIntervalMs: 5 * 60 * 1000 + 2000
});

const MINUTE_TRADE_SIGNAL_DATABASE_OPERATION = Object.freeze({
    CLEAR: "clear",
    THIN: "thin"
});
const MINUTE_TRADE_SIGNAL_MINIMUM_SPACING_SECONDS = BIG_BUY_SIGNAL_DATABASE.timeframeSeconds * 4;

function sanitizeMarket(value, fallback = DEFAULT_MARKET) {
    const normalized = String(value || "").trim().toLowerCase();
    return MARKET_VALUES.includes(normalized) ? normalized : fallback;
}

function getTokenMarket(token, fallback = DEFAULT_MARKET) {
    if (token && typeof token === "object" && token.market) return sanitizeMarket(token.market, fallback);
    const requestSymbol = String(token?.requestSymbol || "").toUpperCase();
    return requestSymbol.startsWith("ALPHA_") ? MARKET.ALPHA : fallback;
}

function getMarketConfig(marketOrToken) {
    const market = typeof marketOrToken === "string"
        ? sanitizeMarket(marketOrToken)
        : getTokenMarket(marketOrToken);
    return MARKET_CONFIG[market] || MARKET_CONFIG[DEFAULT_MARKET];
}

function getMarketLabel(marketOrToken) {
    const market = typeof marketOrToken === "string"
        ? sanitizeMarket(marketOrToken)
        : getTokenMarket(marketOrToken);
    return MARKET_LABELS[market] || MARKET_LABELS[DEFAULT_MARKET];
}

function sanitizeFuturesWebSocketRoute(value) {
    const normalized = String(value || "").trim().toLowerCase();
    return FUTURES_WEBSOCKET_ROUTE_VALUES.includes(normalized) ? normalized : "";
}

function validateFuturesWebSocketRoute(streamName, route) {
    const normalizedStream = String(streamName || "").trim().toLowerCase();
    if (normalizedStream.endsWith("@aggtrade") && route !== FUTURES_WEBSOCKET_ROUTE.MARKET) {
        throw new Error(`Futures aggregate-trade stream requires the /${FUTURES_WEBSOCKET_ROUTE.MARKET} route`);
    }
    if (normalizedStream.includes("@depth") && route !== FUTURES_WEBSOCKET_ROUTE.PUBLIC) {
        throw new Error(`Futures depth stream requires the /${FUTURES_WEBSOCKET_ROUTE.PUBLIC} route`);
    }
}

function createWebSocketStreamDescriptor(token, streamName, options = {}) {
    const market = getTokenMarket(token);
    const config = getMarketConfig(market);
    const normalizedStream = String(streamName || "").trim();
    if (!normalizedStream) throw new Error(`Empty ${getMarketLabel(market)} WebSocket stream name`);
    if (!config.websocketUrl) throw new Error(`Missing ${getMarketLabel(market)} WebSocket endpoint`);

    const mode = config.websocketMode === "direct" ? "direct" : "subscribe";
    if (mode === "subscribe") {
        return Object.freeze({ market, mode, streamName: normalizedStream, url: config.websocketUrl });
    }

    const baseUrl = String(config.websocketUrl).replace(/\/+$/, "");
    if (market === MARKET.FUTURES) {
        const route = sanitizeFuturesWebSocketRoute(options.route);
        if (!route) throw new Error("Futures WebSocket route is required");
        validateFuturesWebSocketRoute(normalizedStream, route);
        return Object.freeze({
            market,
            mode,
            route,
            streamName: normalizedStream,
            url: `${baseUrl}/${route}/ws/${normalizedStream}`
        });
    }

    return Object.freeze({ market, mode, streamName: normalizedStream, url: `${baseUrl}/${normalizedStream}` });
}

const CHART_MAX_CANDLES = 990;
const CHART_TAIL_REFRESH_LIMIT = 10;
const ONE_SECOND_RECONCILE_FETCH_LIMIT = 32;
const ONE_SECOND_RECONCILE_CACHE_TTL_MS = 1500;
const CHART_INDICATOR_INCREMENTAL_EXTRA_BARS = 5;
const CHART_COUNT = 6;
const CHART_CANDLE_TRANSFER_SCHEMA = 5;
const CHART_CANDLE_BASE_TRANSFER_FIELDS = Object.freeze([
    "time", "open", "high", "low", "close", "volume", "takerBuyVolume",
    "sma7", "bbUpper", "bbMiddle", "bbLower",
    "bb2Upper", "bb2Middle", "bb2Lower",
    "hma25", "hma35", "hma55", "hma80", "hma100",
    "threeEma", "devCloudSize",
    "devL1", "devH1", "devL2", "devH2",
    "devL4", "devH4", "devL5", "devH5",
    "devL6", "devH6", "devL8", "devH8",
    "cvdDivSignalCode", "cvdSignalStrength", "cvdSignalConfidenceCode", "cvdSignalReasonCode"
]);
const CHART_CANDLE_DIF_TRANSFER_FIELDS = Object.freeze([
    "dif_rsi", "dif_hmLen2", "dif_hmLen3", "dif_rsi_sma_low", "dif_rsi_sma",
    "dif_env_inner_up", "dif_env_inner_down", "dif_env_outer_up", "dif_env_outer_down",
    "dif_vol", "dif_vol_plot", "dif_low_under_hma", "dif_fill_color_code"
]);
const CHART_CANDLE_TRANSFER_FIELDS = Object.freeze([...CHART_CANDLE_BASE_TRANSFER_FIELDS, ...CHART_CANDLE_DIF_TRANSFER_FIELDS]);
const CHART_CANDLE_BASE_TRANSFER_FIELD_COUNT = CHART_CANDLE_BASE_TRANSFER_FIELDS.length;
const CHART_CANDLE_TRANSFER_FIELD_COUNT = CHART_CANDLE_TRANSFER_FIELDS.length;
const CHART_CANDLE_TRANSFER_FIELD_SET = new Set(CHART_CANDLE_TRANSFER_FIELDS);

const CHART_CANDLE_REQUIRED_TRANSFER_FIELDS = Object.freeze(["time", "open", "high", "low", "close", "volume", "takerBuyVolume"]);
const CHART_CANDLE_RAW_TRANSFER_FIELD_SET = new Set(CHART_CANDLE_REQUIRED_TRANSFER_FIELDS);
const CHART_CANDLE_PRICE_FIELD_GROUPS = Object.freeze({
    sma: Object.freeze(["sma7"]),
    bb: Object.freeze(["bbUpper", "bbMiddle", "bbLower"]),
    bb2: Object.freeze(["bb2Upper", "bb2Middle", "bb2Lower"]),
    hma25: Object.freeze(["hma25"]),
    hma35: Object.freeze(["hma35"]),
    hma55: Object.freeze(["hma55"]),
    hma80: Object.freeze(["hma80"]),
    hma100: Object.freeze(["hma100"]),
    devCloud: Object.freeze([
        "threeEma", "devCloudSize",
        "devL1", "devH1", "devL2", "devH2",
        "devL4", "devH4", "devL5", "devH5",
        "devL6", "devH6", "devL8", "devH8"
    ]),
    cvd: Object.freeze(["cvdDivSignalCode", "cvdSignalStrength", "cvdSignalConfidenceCode", "cvdSignalReasonCode"])
});

function appendUniqueFields(target, fields) {
    for (const field of fields || []) {
        if (CHART_CANDLE_TRANSFER_FIELD_SET.has(field) && !target.includes(field)) target.push(field);
    }
    return target;
}

function getChartTransferFields(options = {}) {
    if (Array.isArray(options.fields) && options.fields.length) {
        const fields = [];
        appendUniqueFields(fields, options.fields);
        return fields.length ? Object.freeze(fields) : CHART_CANDLE_BASE_TRANSFER_FIELDS;
    }
    return options.includeDif === true ? CHART_CANDLE_TRANSFER_FIELDS : CHART_CANDLE_BASE_TRANSFER_FIELDS;
}

function getDecodedTransferFields(encoded, fieldCount) {
    if (Array.isArray(encoded?.fields) && encoded.fields.length === fieldCount) {
        const fields = encoded.fields.filter(field => CHART_CANDLE_TRANSFER_FIELD_SET.has(field));
        if (fields.length === fieldCount) return fields;
    }
    if (fieldCount === CHART_CANDLE_BASE_TRANSFER_FIELD_COUNT) return CHART_CANDLE_BASE_TRANSFER_FIELDS;
    if (fieldCount === CHART_CANDLE_TRANSFER_FIELD_COUNT) return CHART_CANDLE_TRANSFER_FIELDS;
    return null;
}

const METRICS_REASON_CODE = Object.freeze({
    update: 0,
    stream_stopped: 1,
    stream_only_trades: 2,
    aggTrade: 3,
    token_selected: 4,
    settings_updated: 5,
    main_request: 6,
    stream_status: 7,
    metrics_cold: 8,
    diagnostics_timer: 9
});



const DEFAULT_CHART_SETTINGS = Object.freeze({
    schemaVersion: 5,
    heightVh: 40, //30
    zoom: 1.0,
    chartCount: 4,
    horizontalDensityByChartCount: Object.freeze({ 4: 100, 6: 80 }),
    opacity: 0.2,
    inverted: false,
    showMobileControls: true,
    candleTheme: "classic",
    activeChartId: 1,
    splitRatio: "75/25",
    chartTimeframes: Object.freeze(["1s", "1m", "5m", "15m"]),
    chartTimeframesByMarket: Object.freeze({
        [MARKET.ALPHA]: Object.freeze(["1s", "1m", "5m", "15m"]),
        [MARKET.SPOT]: Object.freeze(["1s", "1m", "5m", "15m"]),
        [MARKET.FUTURES]: Object.freeze(["1s", "1m", "5m", "15m"])
    }),
    performance: Object.freeze({
        lightweight1s: true,
        v69Compatible1s: true,
        tailFillThrottleMs: 750
    }),
    indicators: Object.freeze({
        volume: Object.freeze({
            enabled: true,
            upColor: Object.freeze({ color: "rgba(38, 166, 154, 0.2)" }),
            downColor: Object.freeze({ color: "rgba(239, 83, 80, 0.2)" })
        }),
        devCloud: Object.freeze({
            enabled: true,
            emaLength: 132,
            basisMode: "ema",
            lines: Object.freeze({ color: "rgba(102, 153, 212, 0.4)", width: 1 }),
            fill: Object.freeze({ color: "rgba(102, 153, 212, 0.1)" })
        }),
        hma25: Object.freeze({ enabled: true, length: 25, line: Object.freeze({ color: "rgba(0, 100, 255, 0.3)", width: 1 }) }),
        hma35: Object.freeze({ enabled: true, length: 35, line: Object.freeze({ color: "rgba(50, 205, 50, 0.8)", width: 1 }) }),
        hma55: Object.freeze({ enabled: true, length: 55, line: Object.freeze({ color: "rgba(50, 205, 50, 0.8)", width: 1 }) }),
        hma100: Object.freeze({ enabled: true, length: 100, line: Object.freeze({ color: "rgba(50, 205, 50, 0.8)", width: 1 }) }),
        sma: Object.freeze({
            enabled: false,
            crossMarkersEnabled: true,
            crossSoundEnabled: false,
            crossCooldownCandlesByMarket: Object.freeze({
                [MARKET.ALPHA]: 3,
                [MARKET.SPOT]: 3,
                [MARKET.FUTURES]: 3
            }),
            length: 7,
            line: Object.freeze({ color: "rgba(245, 124, 0, 1)", width: 2 })
        }),
        bb: Object.freeze({
            enabled: true,
            length: 16,
            multiplier: 2,
            upper: Object.freeze({ color: "rgba(123, 31, 162, 0.5)", width: 2 }),
            middle: Object.freeze({ color: "rgba(255, 166, 0, 0.7)", width: 2 }),
            lower: Object.freeze({ color: "rgba(123, 31, 162, 0.5)", width: 2 })
        }),
        // Новий Bollinger Bands 2 (за замовчуванням довжина 960):
        bb2: Object.freeze({
            enabled: true,
            length: 960,
            multiplier: 2,
            upper: Object.freeze({ color: "rgba(233, 30, 99, 0.5)", width: 2 }),  // Колір рожевий для контрасту
            middle: Object.freeze({ color: "rgba(156, 39, 176, 0.7)", width: 2 }), // Колір фіолетовий
            lower: Object.freeze({ color: "rgba(233, 30, 99, 0.5)", width: 2 })
        }),
        dif: Object.freeze({
            enabled: false,
            enabledByChartCount: Object.freeze({ 4: false, 6: false }),
            enabledOn1s: true,
            renderMode: "standard",
            rsiLength: 9,
            hmLength: 60,
            hmLength2: 36,
            hmLength3: 22,
            rsiSmaLowLength: 9,
            rsiSmaLength: 31,
            krsi: 30,
            kWidth: 1.0,
            kWidth2: 0.5,
            inverse: false,
            volSmaLen: 100,
            levelsColor: "rgba(180, 180, 180, 0.4)"
        })
    })
});

const SIGNIFICANT_TRADE_THRESHOLD_METHOD = Object.freeze({
    PERCENTILE: "percentile",
    EVT: "evt"
});
const SIGNIFICANT_TRADE_THRESHOLD_METHOD_VALUES = Object.freeze(Object.values(SIGNIFICANT_TRADE_THRESHOLD_METHOD));

function sanitizeSignificantTradeThresholdMethod(value, fallback = SIGNIFICANT_TRADE_THRESHOLD_METHOD.PERCENTILE) {
    const normalized = String(value || "").trim().toLowerCase();
    return SIGNIFICANT_TRADE_THRESHOLD_METHOD_VALUES.includes(normalized) ? normalized : fallback;
}

const DEFAULT_SCALPING_SETTINGS = Object.freeze({
    enabled: false,
    cvdEnabled: true,
    cvdTimeframe: "1s",
    bigTradesEnabled: true,
    bigTradesThresholdUsd: 5000,
    significantTradesThresholdUsd: 10000,
    advancedTradeThresholdsEnabled: false,
    bigBuyMinuteMarkersEnabled: true,
    bigBuyFiveMinuteMarkersEnabled: false,
    bigSellMinuteMarkersEnabled: true,
    advancedTradeThresholdPercentile: 0.91,
    advancedTradeThresholdVolumePercentile: 0.55,
    advancedTradeThresholdTailPercentile: 0.85,
    advancedTradeThresholdMadMultiplier: 1.8,
    advancedTradeThresholdLogSmoothingAlpha: 0.40,
    advancedTradeThresholdMaxDownRatio: 0.80,
    advancedTradeThresholdMaxUpRatio: 2.0,
    advancedSignificantTradeMethod: SIGNIFICANT_TRADE_THRESHOLD_METHOD.PERCENTILE,
    advancedSignificantTradePercentile: 0.995,
    advancedSignificantTradeMinimumSamples: 200,
    advancedSignificantTradeEvtThresholdPercentile: 0.90,
    advancedSignificantTradeEvtMinimumExceedances: 25,
    advancedSignificantTradeHistoryMaxAgeMinutes: 360,
    advancedSignificantTradeLogSmoothingAlpha: 0.35,
    autoAlertsOnBigBuysEnabled: true,
    autoAlertsVolumePercent: 50,
    autoAlertsMinDistancePercent: 0.10,
    cvdAdvancedEnabled: false,
    cvdRollingEnabled: true,
    cvdRollingWindow: 200,
    cvdBandsEnabled: true,
    cvdBandsPeriod: 20,
    cvdBandsMult: 2.0,
    cvdDivergenceEnabled: true,
    cvdDivergenceThreshold: 1.5
});

const ADVANCED_TRADE_THRESHOLD_ALGORITHM_DEFAULTS_BY_MARKET = Object.freeze({
    [MARKET.ALPHA]: Object.freeze({
        advancedTradeThresholdPercentile: 0.91,
        advancedTradeThresholdVolumePercentile: 0.55,
        advancedTradeThresholdTailPercentile: 0.85,
        advancedTradeThresholdMadMultiplier: 1.8,
        advancedTradeThresholdLogSmoothingAlpha: 0.60,
        advancedTradeThresholdMaxDownRatio: 0.80,
        advancedTradeThresholdMaxUpRatio: 2.0,
        advancedSignificantTradeMethod: SIGNIFICANT_TRADE_THRESHOLD_METHOD.PERCENTILE,
        advancedSignificantTradePercentile: 0.995,
        advancedSignificantTradeMinimumSamples: 200,
        advancedSignificantTradeEvtThresholdPercentile: 0.90,
        advancedSignificantTradeEvtMinimumExceedances: 25,
        advancedSignificantTradeHistoryMaxAgeMinutes: 360,
        advancedSignificantTradeLogSmoothingAlpha: 0.38
    }),
    [MARKET.SPOT]: Object.freeze({
        advancedTradeThresholdPercentile: 0.98,
        advancedTradeThresholdVolumePercentile: 0.50,
        advancedTradeThresholdTailPercentile: 0.85,
        advancedTradeThresholdMadMultiplier: 2.0,
        advancedTradeThresholdLogSmoothingAlpha: 0.40,
        advancedTradeThresholdMaxDownRatio: 0.80,
        advancedTradeThresholdMaxUpRatio: 5.0,
        advancedSignificantTradeMethod: SIGNIFICANT_TRADE_THRESHOLD_METHOD.PERCENTILE,
        advancedSignificantTradePercentile: 0.995,
        advancedSignificantTradeMinimumSamples: 200,
        advancedSignificantTradeEvtThresholdPercentile: 0.90,
        advancedSignificantTradeEvtMinimumExceedances: 25,
        advancedSignificantTradeHistoryMaxAgeMinutes: 360,
        advancedSignificantTradeLogSmoothingAlpha: 0.32
    }),
    [MARKET.FUTURES]: Object.freeze({
        advancedTradeThresholdPercentile: 0.98,
        advancedTradeThresholdVolumePercentile: 0.50,
        advancedTradeThresholdTailPercentile: 0.85,
        advancedTradeThresholdMadMultiplier: 2.0,
        advancedTradeThresholdLogSmoothingAlpha: 0.40,
        advancedTradeThresholdMaxDownRatio: 0.80,
        advancedTradeThresholdMaxUpRatio: 5.0,
        advancedSignificantTradeMethod: SIGNIFICANT_TRADE_THRESHOLD_METHOD.PERCENTILE,
        advancedSignificantTradePercentile: 0.995,
        advancedSignificantTradeMinimumSamples: 200,
        advancedSignificantTradeEvtThresholdPercentile: 0.90,
        advancedSignificantTradeEvtMinimumExceedances: 25,
        advancedSignificantTradeHistoryMaxAgeMinutes: 240,
        advancedSignificantTradeLogSmoothingAlpha: 0.35
    })
});

function getDefaultAdvancedTradeThresholdAlgorithmSettings(marketOrToken = DEFAULT_MARKET) {
    const market = typeof marketOrToken === "string"
        ? sanitizeMarket(marketOrToken, DEFAULT_MARKET)
        : getTokenMarket(marketOrToken, state.settings?.activeMarket || DEFAULT_MARKET);
    return ADVANCED_TRADE_THRESHOLD_ALGORITHM_DEFAULTS_BY_MARKET[market]
        || ADVANCED_TRADE_THRESHOLD_ALGORITHM_DEFAULTS_BY_MARKET[DEFAULT_MARKET];
}

function getDefaultScalpingSettingsForMarket(marketOrToken = DEFAULT_MARKET) {
    return {
        ...DEFAULT_SCALPING_SETTINGS,
        ...getDefaultAdvancedTradeThresholdAlgorithmSettings(marketOrToken)
    };
}

function sanitizeAdvancedTradeThresholdAlgorithmSettings(source, marketOrToken = DEFAULT_MARKET) {
    const input = source && typeof source === "object" ? source : {};
    const defaults = getDefaultAdvancedTradeThresholdAlgorithmSettings(marketOrToken);
    return {
        advancedTradeThresholdPercentile: clampNumber(input.advancedTradeThresholdPercentile, 0.50, 0.9999, defaults.advancedTradeThresholdPercentile),
        advancedTradeThresholdVolumePercentile: clampNumber(input.advancedTradeThresholdVolumePercentile, 0.05, 0.99, defaults.advancedTradeThresholdVolumePercentile),
        advancedTradeThresholdTailPercentile: clampNumber(input.advancedTradeThresholdTailPercentile, 0.50, 0.999, defaults.advancedTradeThresholdTailPercentile),
        advancedTradeThresholdMadMultiplier: clampNumber(input.advancedTradeThresholdMadMultiplier, 0.0, 10.0, defaults.advancedTradeThresholdMadMultiplier),
        advancedTradeThresholdLogSmoothingAlpha: clampNumber(input.advancedTradeThresholdLogSmoothingAlpha, 0.01, 1.0, defaults.advancedTradeThresholdLogSmoothingAlpha),
        advancedTradeThresholdMaxDownRatio: clampNumber(input.advancedTradeThresholdMaxDownRatio, 0.05, 1.0, defaults.advancedTradeThresholdMaxDownRatio),
        advancedTradeThresholdMaxUpRatio: clampNumber(input.advancedTradeThresholdMaxUpRatio, 1.0, 100.0, defaults.advancedTradeThresholdMaxUpRatio),
        advancedSignificantTradeMethod: sanitizeSignificantTradeThresholdMethod(input.advancedSignificantTradeMethod, defaults.advancedSignificantTradeMethod),
        advancedSignificantTradePercentile: clampNumber(input.advancedSignificantTradePercentile, 0.900, 0.998, defaults.advancedSignificantTradePercentile),
        advancedSignificantTradeMinimumSamples: Math.max(50, Math.min(5000, Math.floor(Number(input.advancedSignificantTradeMinimumSamples) || defaults.advancedSignificantTradeMinimumSamples))),
        advancedSignificantTradeEvtThresholdPercentile: clampNumber(input.advancedSignificantTradeEvtThresholdPercentile, 0.70, 0.97, defaults.advancedSignificantTradeEvtThresholdPercentile),
        advancedSignificantTradeEvtMinimumExceedances: Math.max(10, Math.min(500, Math.floor(Number(input.advancedSignificantTradeEvtMinimumExceedances) || defaults.advancedSignificantTradeEvtMinimumExceedances))),
        advancedSignificantTradeHistoryMaxAgeMinutes: Math.max(15, Math.min(1440, Math.floor(Number(input.advancedSignificantTradeHistoryMaxAgeMinutes) || defaults.advancedSignificantTradeHistoryMaxAgeMinutes))),
        advancedSignificantTradeLogSmoothingAlpha: clampNumber(input.advancedSignificantTradeLogSmoothingAlpha, 0.05, 1.0, defaults.advancedSignificantTradeLogSmoothingAlpha)
    };
}

function getCurrentAdvancedTradeThresholdAlgorithmSettings(settings = state.settings) {
    const market = sanitizeMarket(
        settings?.activeMarket,
        getTokenMarket(state.selectedToken, DEFAULT_MARKET)
    );
    return sanitizeAdvancedTradeThresholdAlgorithmSettings(settings?.scalping, market);
}

function getAdvancedTradeThresholdFirstLevelAlgorithmSignature(settings = state.settings) {
    const values = getCurrentAdvancedTradeThresholdAlgorithmSettings(settings);
    return [
        values.advancedTradeThresholdPercentile,
        values.advancedTradeThresholdVolumePercentile,
        values.advancedTradeThresholdTailPercentile,
        values.advancedTradeThresholdMadMultiplier,
        values.advancedTradeThresholdLogSmoothingAlpha,
        values.advancedTradeThresholdMaxDownRatio,
        values.advancedTradeThresholdMaxUpRatio
    ].join("|");
}

function getSignificantTradeTailAlgorithmSignature(settings = state.settings) {
    const values = getCurrentAdvancedTradeThresholdAlgorithmSettings(settings);
    return [
        values.advancedSignificantTradeMethod,
        values.advancedSignificantTradePercentile,
        values.advancedSignificantTradeMinimumSamples,
        values.advancedSignificantTradeEvtThresholdPercentile,
        values.advancedSignificantTradeEvtMinimumExceedances,
        values.advancedSignificantTradeHistoryMaxAgeMinutes,
        values.advancedSignificantTradeLogSmoothingAlpha
    ].join("|");
}

const DEFAULT_TURSO_SYNC_SETTINGS = Object.freeze({
    enabled: false,
    thresholdFallbackEnabled: true,
    databaseName: "",
    databaseUrl: "",
    authToken: ""
});

function normalizeTursoDatabaseUrl(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    let normalized = raw;
    if (normalized.startsWith("libsql://")) normalized = `https://${normalized.slice("libsql://".length)}`;
    else if (normalized.startsWith("turso://")) normalized = `https://${normalized.slice("turso://".length)}`;
    if (!/^https:\/\//i.test(normalized)) return "";
    try {
        const url = new URL(normalized);
        url.hash = "";
        url.search = "";
        url.pathname = url.pathname.replace(/\/+$/, "").replace(/\/v2\/pipeline$/i, "") || "/";
        return url.toString().replace(/\/+$/, "");
    } catch (error) {
        return "";
    }
}

function sanitizeTursoSyncSettings(value) {
    const source = value && typeof value === "object" ? value : {};
    return {
        enabled: source.enabled === true,
        thresholdFallbackEnabled: source.thresholdFallbackEnabled !== false,
        databaseName: String(source.databaseName || "").trim().slice(0, 128),
        databaseUrl: normalizeTursoDatabaseUrl(source.databaseUrl),
        authToken: String(source.authToken || "").trim()
    };
}

function isTursoSyncConfigured(settings = state.settings?.tursoSync) {
    const normalized = sanitizeTursoSyncSettings(settings);
    return normalized.enabled && Boolean(normalized.databaseUrl && normalized.authToken);
}

function isTursoThresholdFallbackConfigured(settings = state.settings?.tursoSync) {
    const normalized = sanitizeTursoSyncSettings(settings);
    return normalized.enabled
        && normalized.thresholdFallbackEnabled
        && Boolean(normalized.databaseUrl && normalized.authToken);
}

const DEFAULT_SETTINGS = Object.freeze({
    settingsVersion: 84,
    showEventsLog: false,
    activeMarket: DEFAULT_MARKET,
    selectedRequestSymbol: "",
    selectedRequestSymbols: Object.freeze({
        [MARKET.ALPHA]: "",
        [MARKET.SPOT]: "BTCUSDT",
        [MARKET.FUTURES]: "BTCUSDT"
    }),
    tursoSync: DEFAULT_TURSO_SYNC_SETTINGS,
    tursoTracking: Object.freeze({ previousHeartbeatAt: 0, lastHeartbeatAt: 0, lastPullAt: 0, lastPushAt: 0 }),
    charts: DEFAULT_CHART_SETTINGS,
    scalping: Object.freeze(getDefaultScalpingSettingsForMarket(MARKET.ALPHA))
});

const workerBootId = `${Date.now()}-${Math.random().toString(36).slice(2)}`;

const state = {
    settings: clonePlain(DEFAULT_SETTINGS),
    tokens: [],
    tokenCache: new Map(),
    listingTimeCache: new Map(),
    selectedToken: null,
    paused: false,
    pageVisible: true,
    stream: null,
    charts: new Map(),
    chartHistoryModes: new Map(),
    chartRequestContexts: new Map(),
    chartRequestSerial: 0,
    diagnostics: createDiagnosticsState(),
    workerDiagnostics: createWorkerDiagnosticsState(),
    diagnosticsTimer: null,
    lastMetricsSignatures: Object.create(null),
    lastColdDiagnosticsPostAt: 0,
    lastColdDiagnosticsSignature: "",
    indicatorsOn1sEnabled: false,
    bigBuySignalDatabase: {
        connection: null,
        openPromise: null,
        retryAfter: 0,
        lastErrorAt: 0,
        cleanupTimer: null,
        cleanupInFlight: false,
        loadSerial: 0,
        latestLoadSerialByKey: new Map(),
        inFlightLoads: new Map(),
        maintenanceKeys: new Set(),
        pendingSignalsByKey: new Map()
    },
    tursoSync: {
        flushTimer: null,
        inFlightPromise: null,
        flushPromise: null,
        tokenPullPromises: new Map(),
        schemaReadyKey: "",
        lastStatus: "idle",
        lastError: "",
        lastPullAt: 0,
        lastPushAt: 0
    },
    tursoThresholdFallback: {
        timer: null,
        generation: 0,
        inFlightByKey: new Map(),
        lastCheckAt: 0,
        lastAppliedAt: 0,
        lastError: ""
    }
};

let messageSequence = 0;

function nowMs() { return Date.now(); }
function perfNow() {
    return typeof performance !== "undefined" && typeof performance.now === "function" ? performance.now() : Date.now();
}

const chartIndicatorBufferPool = Object.create(null);

function getReusableFloat64Buffer(key, length) {
    const safeLength = Math.max(0, Math.floor(Number(length) || 0));
    let buffer = chartIndicatorBufferPool[key];
    if (!buffer || buffer.length < safeLength) {
        buffer = new Float64Array(safeLength);
        chartIndicatorBufferPool[key] = buffer;
    }
    return buffer.subarray(0, safeLength);
}

function recordWorkerPerf(name, startedAt, extra = {}) {
    const elapsedMs = Math.max(0, perfNow() - startedAt);
    const perf = state.workerDiagnostics.perf || (state.workerDiagnostics.perf = Object.create(null));
    const item = perf[name] || { count: 0, lastMs: 0, maxMs: 0, avgMs: 0, lastAt: 0 };
    item.count += 1;
    item.lastMs = Number(elapsedMs.toFixed(3));
    item.maxMs = Number(Math.max(item.maxMs || 0, elapsedMs).toFixed(3));
    item.avgMs = Number((((item.avgMs || 0) * (item.count - 1) + elapsedMs) / item.count).toFixed(3));
    item.lastAt = nowMs();
    item.last = extra;
    perf[name] = item;
    return elapsedMs;
}

function clonePlain(value) {
    return JSON.parse(JSON.stringify(value));
}

function post(type, payload = {}, transferList = []) {
    const message = {
        type,
        workerVersion: WORKER_VERSION,
        workerBootId,
        seq: ++messageSequence,
        sentAt: nowMs(),
        payload
    };
    if (Array.isArray(transferList) && transferList.length > 0) {
        self.postMessage(message, transferList);
    } else {
        self.postMessage(message);
    }
}

function reply(requestId, ok, payload = {}, transferList = []) {
    post(ok ? "REPLY" : "REPLY_ERROR", { requestId, ok, ...payload }, transferList);
}


function isFiniteNumberValue(value) {
    return Number.isFinite(Number(value));
}

function numberOrNaN(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : NaN;
}

function normalizeCvdDivSignalCode(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return 0;
    if (numeric > 0) return 1;
    if (numeric < 0) return -1;
    return 0;
}

function getCvdDivSignalCode(candle) {
    if (!candle || typeof candle !== "object") return 0;
    if (candle.cvdDivSignalCode !== undefined && candle.cvdDivSignalCode !== null) {
        return normalizeCvdDivSignalCode(candle.cvdDivSignalCode);
    }
    if (candle.cvdDivSignal === "bullish_absorption") return 1;
    if (candle.cvdDivSignal === "bearish_absorption") return -1;
    return 0;
}

function decodeCvdDivSignal(code) {
    const normalized = normalizeCvdDivSignalCode(code);
    if (normalized > 0) return "bullish_absorption";
    if (normalized < 0) return "bearish_absorption";
    return null;
}

function hydrateCvdDivSignal(candle) {
    if (!candle || typeof candle !== "object") return candle;
    const code = getCvdDivSignalCode(candle);
    candle.cvdDivSignalCode = code;
    candle.cvdDivSignal = decodeCvdDivSignal(code);
    return candle;
}

function getTransferCandleValue(candle, field) {
    if (field === "cvdDivSignalCode") return getCvdDivSignalCode(candle);
    return numberOrNaN(candle?.[field]);
}



function getMetricsReasonCode(reason) {
    return METRICS_REASON_CODE[reason] ?? METRICS_REASON_CODE.update;
}


function encodeCandlesForTransfer(candles, options = {}) {
    const source = Array.isArray(candles) ? candles : [];
    const fields = getChartTransferFields(options);
    const fieldCount = fields.length;
    const data = new Float64Array(source.length * fieldCount);
    for (let row = 0; row < source.length; row += 1) {
        const candle = source[row] || {};
        const offset = row * fieldCount;
        for (let col = 0; col < fieldCount; col += 1) {
            data[offset + col] = getTransferCandleValue(candle, fields[col]);
        }
    }
    return {
        encoded: {
            schema: CHART_CANDLE_TRANSFER_SCHEMA,
            fieldCount,
            fields,
            includesDif: options.includeDif === true,
            length: source.length,
            buffer: data.buffer
        },
        transferList: [data.buffer]
    };
}

function decodeCandlesFromTransfer(encoded) {
    if (!encoded || !encoded.buffer) return null;
    const schema = Number(encoded.schema) || 0;
    if (schema !== CHART_CANDLE_TRANSFER_SCHEMA && schema !== 4) return null;
    const length = Math.max(0, Math.floor(Number(encoded.length) || 0));
    const fieldCount = Math.max(0, Math.floor(Number(encoded.fieldCount) || 0));
    const fields = schema === 4 ? CHART_CANDLE_TRANSFER_FIELDS : getDecodedTransferFields(encoded, fieldCount);
    if (!fields || fields.length !== fieldCount) return null;
    const data = new Float64Array(encoded.buffer);
    if (data.length < length * fieldCount) return null;
    const candles = new Array(length);
    for (let row = 0; row < length; row += 1) {
        const candle = {};
        const offset = row * fieldCount;
        for (let col = 0; col < fieldCount; col += 1) {
            const value = data[offset + col];
            if (Number.isFinite(value)) candle[fields[col]] = value;
        }
        candles[row] = candle;
    }
    return candles;
}

function hydrateIncomingPayload(payload) {
    if (!payload || typeof payload !== "object") return payload || {};
    if (payload.rawCandlesF64 && !payload.rawCandles) {
        const rawCandles = decodeCandlesFromTransfer(payload.rawCandlesF64);
        if (rawCandles) {
            const next = { ...payload, rawCandles };
            delete next.rawCandlesF64;
            return next;
        }
    }
    return payload;
}

function prepareChartReplyPayload(result) {
    if (!result || typeof result !== "object") return { payload: result || {}, transferList: [] };
    const payload = { ...result };
    const transferList = [];
    const collections = [
        { sourceKey: "candles", encodedKey: "candlesF64", fields: result.transferFields, includeDif: result.includesDif === true },
        { sourceKey: "indicatorCandles", encodedKey: "indicatorCandlesF64", fields: result.indicatorTransferFields, includeDif: result.includesDif === true },
        { sourceKey: "rawCorrections", encodedKey: "rawCorrectionsF64", fields: CHART_CANDLE_REQUIRED_TRANSFER_FIELDS, includeDif: false }
    ];
    for (const collection of collections) {
        const source = result[collection.sourceKey];
        if (!Array.isArray(source)) continue;
        const encodedResult = encodeCandlesForTransfer(source, {
            includeDif: collection.includeDif,
            fields: collection.fields
        });
        payload[collection.encodedKey] = encodedResult.encoded;
        transferList.push(...encodedResult.transferList);
        delete payload[collection.sourceKey];
    }
    return { payload, transferList };
}

function replyChartResult(requestId, result) {
    const prepared = prepareChartReplyPayload(result);
    reply(requestId, true, prepared.payload, prepared.transferList);
}

function getDiagnosticsSignature(snapshot) {
    if (!snapshot) return "null";
    const stream = snapshot.streamDiagnostics || {};
    const streams = stream.streams || {};
    return [
        snapshot.workerStatus || "",
        snapshot.wsState || "",
        snapshot.subscriptionStatus || "",
        snapshot.wsOpenedAt || 0,
        snapshot.wsLastMessageAt || 0,
        streams.aggTrade?.received || 0,
        streams.aggTrade?.applied || 0,
        streams.aggTrade?.rejected || 0
    ].join("|");
}

function postError(key, error, payload = null) {
    recordError(key, error, payload);
    post("WORKER_ERROR", {
        key,
        message: error?.message || String(error || "Unknown error"),
        diagnostics: getWorkerDiagnosticsSnapshot()
    });
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function clampNumber(value, min, max, fallback) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return fallback;
    return Math.min(max, Math.max(min, numeric));
}

function clampInteger(value, min, max, fallback) {
    return Math.round(clampNumber(value, min, max, fallback));
}

function getDepthIntervalSetting(value, marketOrToken = state.selectedToken || state.settings.activeMarket) {
    const config = getMarketConfig(marketOrToken);
    const interval = String(value || "").trim();
    return config.allowedDepthIntervals.includes(interval) ? interval : config.depthIntervalDefault;
}


function createStreamDiagnostics() {
    return {
        received: 0,
        applied: 0,
        rejected: 0,
        errors: 0,
        lastReceivedAt: null,
        lastAppliedAt: null,
        lastRejectedAt: null,
        lastRejectReason: ""
    };
}

function createDiagnosticsState() {
    return {
        sessionId: 0,
        token: "—",
        requestSymbol: "—",
        streamSymbol: "—",
        wsState: "idle",
        subscriptionStatus: "idle",
        subscriptionId: null,
        activeStreams: [],
        startedAt: null,
        streams: {
            aggTrade: createStreamDiagnostics(),
            ack: createStreamDiagnostics(),
            wsError: createStreamDiagnostics(),
            unknown: createStreamDiagnostics()
        },
        errors: {
            total: 0,
            suppressed: 0,
            lastErrorAt: null,
            lastError: "",
            byKey: Object.create(null)
        }
    };
}

function createWorkerDiagnosticsState() {
    return {
        workerStatus: "booting",
        workerBootId,
        workerVersion: WORKER_VERSION,
        workerGeneration: 0,
        startedAt: nowMs(),
        lastMainMessageAt: null,
        lastWorkerMessageAt: null,
        lastPingAt: null,
        lastPongAt: null,
        lastPongLatencyMs: null,
        lastWorkerError: "",
        lastWorkerRestartReason: "",
        lastWorkerRestartAt: null,
        workerHiddenSince: null,
        workerResumeCount: 0,
        workerUnexpectedSilenceCount: 0,
        chartBuffers: Object.create(null),
        perf: Object.create(null)
    };
}

function resetDiagnosticsForToken(token) {
    const next = createDiagnosticsState();
    next.sessionId = (state.diagnostics?.sessionId || 0) + 1;
    next.token = token?.symbol || "—";
    next.requestSymbol = getRequestSymbol(token) || "—";
    next.streamSymbol = getStreamSymbol(token) || "—";
    next.startedAt = nowMs();
    state.diagnostics = next;
}

function getStreamDiagnostics(kind) {
    const diag = state.diagnostics;
    if (!diag.streams[kind]) diag.streams[kind] = createStreamDiagnostics();
    return diag.streams[kind];
}

function markStreamReceived(kind) {
    const item = getStreamDiagnostics(kind);
    item.received += 1;
    item.lastReceivedAt = nowMs();
}

function markStreamApplied(kind) {
    const item = getStreamDiagnostics(kind);
    item.applied += 1;
    item.lastAppliedAt = nowMs();
}

function markStreamRejected(kind, reason) {
    const item = getStreamDiagnostics(kind);
    item.rejected += 1;
    item.lastRejectedAt = nowMs();
    item.lastRejectReason = reason || "unknown";
}

function safePayloadSample(payload) {
    try {
        const raw = typeof payload === "string" ? payload : JSON.stringify(payload);
        if (!raw) return "";
        return raw.length > CONFIG.diagnosticsMaxPayloadChars
            ? raw.slice(0, CONFIG.diagnosticsMaxPayloadChars) + "…"
            : raw;
    } catch {
        return "[unserializable payload]";
    }
}

function recordError(key, error, payload = null) {
    const diag = state.diagnostics;
    const now = nowMs();
    const shortMessage = String(error?.message || error || "Unknown error");
    const record = diag.errors.byKey[key] || { count: 0, lastLoggedAt: 0, suppressed: 0 };
    record.count += 1;
    diag.errors.total += 1;
    diag.errors.lastErrorAt = now;
    diag.errors.lastError = `${key}: ${shortMessage}`;
    state.workerDiagnostics.lastWorkerError = diag.errors.lastError;

    const shouldLog = record.count === 1 || (now - record.lastLoggedAt) >= CONFIG.diagnosticsErrorRepeatMs;
    if (shouldLog) {
        record.lastLoggedAt = now;
        record.suppressed = 0;
    } else {
        record.suppressed += 1;
        diag.errors.suppressed += 1;
    }
    diag.errors.byKey[key] = record;
}

function updateWsDiagnostics(stateText) {
    state.diagnostics.wsState = stateText;
}

function updateSubscriptionDiagnostics(status, details = {}) {
    state.diagnostics.subscriptionStatus = status;
    if (details.id !== undefined) state.diagnostics.subscriptionId = details.id;
    if (Array.isArray(details.streams)) state.diagnostics.activeStreams = details.streams.slice();
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 10000) {
    const externalSignal = options.signal || null;
    if (externalSignal?.aborted) {
        throw externalSignal.reason instanceof Error
            ? externalSignal.reason
            : new DOMException("Aborted", "AbortError");
    }
    const controller = new AbortController();
    const forwardAbort = () => {
        if (controller.signal.aborted) return;
        try {
            controller.abort(externalSignal?.reason);
        } catch {
            controller.abort();
        }
    };
    const timer = setTimeout(() => {
        if (!controller.signal.aborted) controller.abort(new DOMException("Request timeout", "TimeoutError"));
    }, timeoutMs);
    if (externalSignal) {
        if (externalSignal.aborted) forwardAbort();
        else externalSignal.addEventListener("abort", forwardAbort, { once: true });
    }
    try {
        const response = await fetch(url, { ...options, signal: controller.signal, cache: "no-store" });
        if (!response.ok) {
            let responseBody = "";
            try {
                responseBody = String(await response.text()).slice(0, CONFIG.diagnosticsMaxPayloadChars);
            } catch {
                responseBody = "";
            }
            const error = new Error(`HTTP ${response.status}${responseBody ? ` · ${responseBody}` : ""}`);
            error.name = "HttpResponseError";
            error.status = Number(response.status) || 0;
            error.statusText = String(response.statusText || "");
            error.responseBody = responseBody;
            error.requestUrl = String(url);
            throw error;
        }
        return await response.json();
    } finally {
        clearTimeout(timer);
        externalSignal?.removeEventListener?.("abort", forwardAbort);
    }
}

function normalizeAlphaId(raw) {
    if (raw === null || raw === undefined) return "";
    const str = String(raw).trim();
    if (!str) return "";
    return str.startsWith("ALPHA_") ? str : "ALPHA_" + str.replace(/^ALPHA[_-]?/i, "");
}

function getRequestSymbol(token) {
    return token ? String(token.requestSymbol || "").trim().toUpperCase() : "";
}

function getStreamSymbol(token) {
    const explicit = String(token?.streamSymbol || "").trim().toLowerCase();
    return explicit || getRequestSymbol(token).toLowerCase();
}

function getTokenKey(token) {
    const requestSymbol = getRequestSymbol(token);
    return requestSymbol ? `${getTokenMarket(token)}:${requestSymbol}` : "";
}

function clampPricePrecision(value, fallback = 8) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return fallback;
    return Math.max(0, Math.min(12, Math.round(numeric)));
}

function getPrecisionFromTickSizeValue(tickSize) {
    const text = String(tickSize ?? "").trim();
    if (!text || !Number.isFinite(Number(text)) || Number(text) <= 0) return null;
    if (text.includes("e") || text.includes("E")) {
        const value = Number(text);
        for (let precision = 0; precision <= 12; precision += 1) {
            const scaled = value * Math.pow(10, precision);
            if (Math.abs(scaled - Math.round(scaled)) < 1e-8) return precision;
        }
        return 8;
    }
    const dotIndex = text.indexOf(".");
    if (dotIndex < 0) return 0;
    const decimals = text.slice(dotIndex + 1).replace(/0+$/, "");
    return clampPricePrecision(decimals.length, 8);
}

function resolveTokenPricePrecision(raw) {
    const directCandidates = [
        raw?.pricePrecision,
        raw?.priceDecimal,
        raw?.priceDecimals,
        raw?.tradeDecimal,
        raw?.quotePrecision
    ];
    for (const candidate of directCandidates) {
        const numeric = Number(candidate);
        if (Number.isFinite(numeric) && numeric >= 0) return clampPricePrecision(numeric, 8);
    }
    const tickSize = raw?.tickSize
        ?? raw?.priceTickSize
        ?? raw?.filters?.find?.(filter => filter?.filterType === "PRICE_FILTER")?.tickSize;
    const tickPrecision = getPrecisionFromTickSizeValue(tickSize);
    return tickPrecision === null ? 8 : tickPrecision;
}

function sanitizeText(text) {
    return String(text ?? "").replace(/[\u0000-\u001f\u007f]/g, "").trim();
}

function normalizeAlphaToken(raw, index) {
    const symbol = sanitizeText(raw.symbol || raw.baseAsset || raw.tokenSymbol || "").toUpperCase();
    const name = sanitizeText(raw.name || raw.fullName || symbol);
    const alphaId = normalizeAlphaId(raw.alphaId ?? raw.tokenId ?? raw.id ?? raw.alphaTokenId);
    if (!symbol || !alphaId) return null;

    const listingTime = normalizeEpochMilliseconds(raw.listingTime ?? raw.onboardDate ?? raw.openTime ?? 0);
    const age = listingTime > 0 ? nowMs() - listingTime : Infinity;
    const group = age < CONFIG.newCoinMs ? 0 : age < CONFIG.midCoinMs ? 1 : 2;
    const tradeDecimal = Number(raw.tradeDecimal ?? raw.quantityPrecision ?? raw.pricePrecision ?? 8);
    const pricePrecision = resolveTokenPricePrecision(raw);
    const requestSymbol = alphaId + "USDT";

    return {
        market: MARKET.ALPHA,
        marketLabel: MARKET_LABELS[MARKET.ALPHA],
        symbol,
        baseAsset: symbol,
        quoteAsset: "USDT",
        name,
        alphaId,
        requestSymbol,
        streamSymbol: requestSymbol.toLowerCase(),
        listingTime,
        tradeDecimal: Number.isFinite(tradeDecimal) ? tradeDecimal : 8,
        pricePrecision,
        group,
        isNew: group === 0,
        isMid: group === 1,
        sourceIndex: index
    };
}

function normalizeExchangeToken(raw, index, market) {
    const requestSymbol = sanitizeText(raw?.symbol).toUpperCase();
    const baseAsset = sanitizeText(raw?.baseAsset).toUpperCase();
    const quoteAsset = sanitizeText(raw?.quoteAsset).toUpperCase();
    if (!requestSymbol || !baseAsset || quoteAsset !== "USDT") return null;

    const pricePrecision = resolveTokenPricePrecision(raw);
    const quantityPrecision = Number(raw?.quantityPrecision ?? raw?.baseAssetPrecision ?? 8);
    const listingTime = normalizeEpochMilliseconds(raw?.onboardDate ?? raw?.listingTime ?? raw?.openTime ?? 0);
    const listingTimeSource = listingTime > 0
        ? (raw?.onboardDate ? "onboard_date" : "exchange_metadata")
        : "";
    return {
        market,
        marketLabel: MARKET_LABELS[market],
        symbol: baseAsset,
        baseAsset,
        quoteAsset,
        name: baseAsset,
        requestSymbol,
        streamSymbol: requestSymbol.toLowerCase(),
        listingTime,
        listingTimeSource,
        tradeDecimal: Number.isFinite(quantityPrecision) ? quantityPrecision : 8,
        pricePrecision,
        group: 2,
        isNew: false,
        isMid: false,
        sourceIndex: index
    };
}

function isTradableExchangeSymbol(raw, market) {
    if (!raw || raw.status !== "TRADING" || raw.quoteAsset !== "USDT") return false;
    if (market === MARKET.SPOT) return raw.isSpotTradingAllowed !== false;
    if (market === MARKET.FUTURES) return raw.contractType === "PERPETUAL";
    return false;
}

async function fetchTokensForMarket(market) {
    const config = getMarketConfig(market);
    if (market === MARKET.ALPHA) {
        const payload = await fetchJsonWithTimeout(config.tokenListUrl, {}, 12000);
        const rawList = Array.isArray(payload?.data) ? payload.data : [];
        return rawList
            .filter(item => !item.cexOffDisplay && !item.offline)
            .map(normalizeAlphaToken)
            .filter(Boolean)
            .sort((a, b) => (a.group - b.group) || a.symbol.localeCompare(b.symbol));
    }

    const payload = await fetchJsonWithTimeout(config.exchangeInfoUrl, {}, 15000);
    const rawList = Array.isArray(payload?.symbols) ? payload.symbols : [];
    return rawList
        .filter(item => isTradableExchangeSymbol(item, market))
        .map((item, index) => normalizeExchangeToken(item, index, market))
        .filter(Boolean)
        .sort((a, b) => a.symbol.localeCompare(b.symbol) || a.requestSymbol.localeCompare(b.requestSymbol));
}

async function loadTokensInWorker(rawMarket = state.settings.activeMarket, options = {}) {
    const market = sanitizeMarket(rawMarket);
    const forceRefresh = options.forceRefresh === true;
    const cached = state.tokenCache.get(market);
    const cacheFresh = cached && (nowMs() - cached.fetchedAt) < CONFIG.tokenCacheTtlMs;
    if (!forceRefresh && cacheFresh && Array.isArray(cached.tokens) && cached.tokens.length > 0) {
        state.tokens = cached.tokens;
        updateWorkerStatus("ready");
        return cached.tokens;
    }

    updateWorkerStatus(`loading_tokens_${market}`);
    const tokens = await fetchTokensForMarket(market);
    if (tokens.length === 0) throw new Error(`${getMarketLabel(market)} token list is empty`);
    state.tokenCache.set(market, { tokens, fetchedAt: nowMs() });
    state.tokens = tokens;
    updateWorkerStatus("ready");
    return tokens;
}

class OrderBook {
    constructor(market = DEFAULT_MARKET) {
        this.market = sanitizeMarket(market);
        this.reset();
    }

    setMarket(market) {
        this.market = sanitizeMarket(market);
        this.reset();
    }

    reset() {
        this.bids = new Map();
        this.asks = new Map();
        this.bidLevels = [];
        this.askLevels = [];
        this.lastUpdateId = 0;
        this.lastEventTime = null;
        this.snapshotLoaded = false;
        this.resyncing = false;
        this.hasAppliedLiveDelta = false;
        this.lastResetAt = nowMs();
        this.changeSeq = 0;
    }

    loadSnapshot(data) {
        const bidSide = createBookSideFromLevels(data?.bids, "bid");
        const askSide = createBookSideFromLevels(data?.asks, "ask");
        this.bids = bidSide.map;
        this.asks = askSide.map;
        this.bidLevels = bidSide.levels;
        this.askLevels = askSide.levels;
        this.lastUpdateId = Number(data?.lastUpdateId) || 0;
        this.lastEventTime = Number(data?.E || data?.T) || null;
        this.snapshotLoaded = true;
        this.resyncing = false;
        this.hasAppliedLiveDelta = false;
        this.lastResetAt = nowMs();
        this.changeSeq = 0;
    }

    applyDelta(delta) {
        if (!this.snapshotLoaded) return { applied: false, reason: "no_snapshot" };
        const firstUpdateId = Number(delta?.U) || null;
        const finalUpdateId = Number(delta?.u) || null;
        const previousUpdateId = Number(delta?.pu) || null;

        if (!finalUpdateId) return { applied: false, reason: "invalid_update_id", firstUpdateId, finalUpdateId, previousUpdateId };
        if (finalUpdateId <= this.lastUpdateId) return { applied: false, reason: "old_update", firstUpdateId, finalUpdateId, previousUpdateId };

        const expectedNextId = this.lastUpdateId + 1;
        if (!this.hasAppliedLiveDelta) {
            const coversBoundary = Boolean(firstUpdateId && firstUpdateId <= expectedNextId && finalUpdateId >= expectedNextId);
            const futuresBoundary = this.market === MARKET.FUTURES
                && Boolean(firstUpdateId && firstUpdateId <= this.lastUpdateId && finalUpdateId >= this.lastUpdateId);
            const previousMatchesSnapshot = Boolean(previousUpdateId && previousUpdateId === this.lastUpdateId);
            if (!coversBoundary && !futuresBoundary && !previousMatchesSnapshot) {
                return {
                    applied: false,
                    reason: "sequence_gap",
                    firstUpdateId,
                    finalUpdateId,
                    previousUpdateId,
                    expected: this.lastUpdateId,
                    bootstrap: true
                };
            }
        } else if (this.market === MARKET.FUTURES) {
            if (!previousUpdateId || previousUpdateId !== this.lastUpdateId) {
                return { applied: false, reason: "sequence_gap", firstUpdateId, finalUpdateId, previousUpdateId, expected: this.lastUpdateId };
            }
        } else {
            if (previousUpdateId && previousUpdateId !== this.lastUpdateId) {
                return { applied: false, reason: "sequence_gap", firstUpdateId, finalUpdateId, previousUpdateId, expected: this.lastUpdateId };
            }
            if (!previousUpdateId && firstUpdateId && firstUpdateId > expectedNextId) {
                return { applied: false, reason: "possible_gap", firstUpdateId, finalUpdateId, previousUpdateId, expected: expectedNextId };
            }
        }

        this.applySideLevels(this.bids, this.bidLevels, delta?.b, "bid");
        this.applySideLevels(this.asks, this.askLevels, delta?.a, "ask");
        this.lastUpdateId = finalUpdateId;
        this.lastEventTime = Number(delta?.E || delta?.T) || this.lastEventTime;
        this.hasAppliedLiveDelta = true;
        this.changeSeq = (Number(this.changeSeq) || 0) + 1;
        return { applied: true, firstUpdateId, finalUpdateId, previousUpdateId };
    }

    applySideLevels(map, sortedLevels, updates, side) {
        if (!Array.isArray(updates) || updates.length === 0) return null;

        for (const rawLevel of updates) {
            const parsed = parseBookLevel(rawLevel);
            if (!parsed || !Number.isFinite(parsed.price) || parsed.price <= 0 || !Number.isFinite(parsed.qty)) continue;

            const existing = map.get(parsed.priceKey);
            if (parsed.qty <= 0) {
                if (!existing) continue;
                map.delete(parsed.priceKey);
                removeSortedLevel(sortedLevels, existing, side);
                continue;
            }

            if (existing) {
                if (existing.qty === parsed.qty) continue;
                existing.qty = parsed.qty;
                existing.notional = parsed.price * parsed.qty;
                continue;
            }

            const next = {
                priceKey: parsed.priceKey,
                price: parsed.price,
                qty: parsed.qty,
                notional: parsed.price * parsed.qty
            };
            map.set(next.priceKey, next);
            insertSortedLevel(sortedLevels, next, side);
        }

    }






    sortedBids() {
        return this.bidLevels;
    }

    sortedAsks() {
        return this.askLevels;
    }
}

function normalizeTopN(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return 0;
    return Math.floor(n);
}

function createBookSideFromLevels(levels, side) {
    const map = new Map();
    const output = [];
    if (!Array.isArray(levels)) return { map, levels: output };

    for (const rawLevel of levels) {
        const parsed = parseBookLevel(rawLevel);
        if (!parsed || parsed.price <= 0 || parsed.qty <= 0) continue;
        const level = {
            priceKey: parsed.priceKey,
            price: parsed.price,
            qty: parsed.qty,
            notional: parsed.price * parsed.qty
        };
        map.set(level.priceKey, level);
        output.push(level);
    }
    output.sort((a, b) => compareLevelForSide(a, b, side));
    return { map, levels: output };
}

function parseBookLevel(level) {
    if (!Array.isArray(level) || level.length < 2) return null;
    const priceText = String(level[0]);
    const qtyText = String(level[1]);
    const price = Number(priceText);
    const qty = Number(qtyText);
    if (!Number.isFinite(price) || !Number.isFinite(qty)) return null;
    return { priceKey: priceText, price, qty };
}

function compareBidLevels(a, b) {
    return b.price - a.price;
}

function compareAskLevels(a, b) {
    return a.price - b.price;
}

function compareLevelForSide(a, b, side) {
    return side === "bid" ? compareBidLevels(a, b) : compareAskLevels(a, b);
}

function findInsertionIndex(sortedLevels, level, side) {
    let left = 0;
    let right = sortedLevels.length;
    while (left < right) {
        const mid = (left + right) >> 1;
        if (compareLevelForSide(level, sortedLevels[mid], side) < 0) right = mid;
        else left = mid + 1;
    }
    return left;
}

function insertSortedLevel(sortedLevels, level, side) {
    const index = findInsertionIndex(sortedLevels, level, side);
    sortedLevels.splice(index, 0, level);
}

function removeSortedLevel(sortedLevels, level, side) {
    const index = sortedLevels.indexOf(level);
    if (index >= 0) {
        sortedLevels.splice(index, 1);
        return true;
    }
    const priceKey = level?.priceKey;
    const fallbackIndex = sortedLevels.findIndex(item => item.priceKey === priceKey);
    if (fallbackIndex >= 0) {
        sortedLevels.splice(fallbackIndex, 1);
        return true;
    }
    return false;
}







function normalizeAggTradeBuyerMakerFlag(value) {
    if (value === true || value === false) return value;
    if (typeof value === "string") {
        const normalized = value.trim().toLowerCase();
        if (normalized === "true") return true;
        if (normalized === "false") return false;
    }
    if (typeof value === "number") {
        if (value === 1) return true;
        if (value === 0) return false;
    }
    return null;
}

function isAlphaTokenActive() {
    return getTokenMarket(state.selectedToken) === MARKET.ALPHA;
}

function getAggTradeSide(trade) {
    const isAlpha = isAlphaTokenActive();
    const isBuyerMaker = !isAlpha ? normalizeAggTradeBuyerMakerFlag(trade?.m) : null;
    if (isBuyerMaker === true) return "sell";
    if (isBuyerMaker === false) return "buy";

    const price = Number(trade?.p);
    if (Number.isFinite(price) && state.scalpingState) {
        if (state.scalpingState.lastPrice !== null && state.scalpingState.lastPrice !== undefined) {
            if (price > state.scalpingState.lastPrice) {
                state.scalpingState.lastSide = "buy";
            } else if (price < state.scalpingState.lastPrice) {
                state.scalpingState.lastSide = "sell";
            }
        }
        state.scalpingState.lastPrice = price;
        return state.scalpingState.lastSide || "buy";
    }
    return "unknown";
}

function getTimeframeSeconds(tf) {
    const value = String(tf || "");
    const n = Number.parseInt(value, 10);
    if (!Number.isFinite(n) || n <= 0) return 0;
    if (value.endsWith("s")) return n;
    if (value.endsWith("m")) return n * 60;
    if (value.endsWith("h")) return n * 60 * 60;
    if (value.endsWith("d")) return n * 24 * 60 * 60;
    if (value.endsWith("w")) return n * 7 * 24 * 60 * 60;
    return 0;
}

function normalizeEpochMilliseconds(value) {
    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
        if (numeric <= 0) return 0;
        return numeric < 1e12 ? Math.floor(numeric * 1000) : Math.floor(numeric);
    }
    const parsed = Date.parse(String(value ?? "").trim());
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 0;
}

function getTokenListingTimeMs(token) {
    if (!token || typeof token !== "object") return 0;
    return normalizeEpochMilliseconds(token.listingTime ?? token.onboardDate ?? token.openTime ?? 0);
}

const listingTimeResolutionInFlight = new Map();

function getListingTimeCacheKey(token) {
    const requestSymbol = getRequestSymbol(token);
    return requestSymbol ? `${getTokenMarket(token)}:${requestSymbol}` : "";
}

function persistResolvedListingTime(token, listingTime, source = "") {
    const normalized = normalizeEpochMilliseconds(listingTime);
    if (!token || normalized <= 0) return 0;
    token.listingTime = normalized;
    if (source) token.listingTimeSource = String(source);
    const cacheKey = getListingTimeCacheKey(token);
    if (cacheKey) {
        state.listingTimeCache.set(cacheKey, {
            listingTime: normalized,
            source: String(source || token.listingTimeSource || "resolved"),
            resolvedAt: nowMs()
        });
    }
    return normalized;
}

function getCachedListingTimeRecord(token) {
    const cacheKey = getListingTimeCacheKey(token);
    if (!cacheKey) return null;
    const cached = state.listingTimeCache.get(cacheKey);
    const listingTime = normalizeEpochMilliseconds(cached?.listingTime);
    return listingTime > 0 ? { listingTime, source: String(cached?.source || "cache") } : null;
}

function getEarliestOneMinuteKlineUrl(token) {
    const config = getMarketConfig(token);
    const symbol = encodeURIComponent(getRequestSymbol(token));
    // startTime=0 is intentional. The generic URL builder omits zero timestamps,
    // while Binance uses an explicit startTime to return the oldest available rows.
    return `${config.klinesUrl}?symbol=${symbol}&interval=1m&startTime=0&limit=1`;
}

async function fetchEarliestOneMinuteKlineTime(token) {
    const market = getTokenMarket(token);
    if (market !== MARKET.SPOT && market !== MARKET.FUTURES) return 0;
    const payload = await fetchJsonWithTimeout(getEarliestOneMinuteKlineUrl(token), {}, 12000);
    const rows = market === MARKET.ALPHA ? (payload?.data || payload) : payload;
    if (!Array.isArray(rows) || rows.length === 0 || !Array.isArray(rows[0])) return 0;
    return normalizeEpochMilliseconds(rows[0][0]);
}

async function resolveTokenListingTimeRecord(token, signal = null) {
    if (!token || typeof token !== "object") return { listingTime: 0, source: "missing_token" };

    const metadataListingTime = getTokenListingTimeMs(token);
    const metadataSource = metadataListingTime > 0
        ? (token.listingTimeSource || (token.onboardDate ? "onboard_date" : "exchange_metadata"))
        : "";
    const market = getTokenMarket(token);
    if (market === MARKET.ALPHA) {
        if (metadataListingTime > 0) persistResolvedListingTime(token, metadataListingTime, metadataSource || "alpha_metadata");
        return { listingTime: metadataListingTime, source: metadataSource || "alpha_metadata" };
    }

    const cached = getCachedListingTimeRecord(token);
    if (cached) {
        persistResolvedListingTime(token, cached.listingTime, cached.source);
        return cached;
    }

    // Prefer an explicit exchange-provided listing timestamp when the symbol payload
    // contains one. The earliest 1m kline request is the authoritative public fallback
    // for Spot symbols whose exchange-information payload has no timestamp.
    if (metadataListingTime > 0) {
        const listingTime = persistResolvedListingTime(token, metadataListingTime, metadataSource || "exchange_metadata");
        return { listingTime, source: metadataSource || "exchange_metadata" };
    }

    const cacheKey = getListingTimeCacheKey(token);
    if (!cacheKey) return { listingTime: metadataListingTime, source: metadataSource || "missing_symbol" };

    let request = listingTimeResolutionInFlight.get(cacheKey);
    if (!request) {
        request = (async () => {
            try {
                const firstKlineTime = await fetchEarliestOneMinuteKlineTime(token);
                if (firstKlineTime > 0) {
                    const listingTime = persistResolvedListingTime(token, firstKlineTime, "first_1m_kline");
                    return { listingTime, source: "first_1m_kline" };
                }
            } catch (error) {
                if (error?.name !== "AbortError") {
                    console.warn(`Failed to resolve earliest 1m kline for ${cacheKey}:`, error);
                }
            }
            const fallbackSource = metadataSource || (metadataListingTime > 0 ? "exchange_metadata" : "unresolved");
            const listingTime = persistResolvedListingTime(token, metadataListingTime, fallbackSource);
            return { listingTime, source: fallbackSource };
        })();
        listingTimeResolutionInFlight.set(cacheKey, request);
        void request.finally(() => {
            if (listingTimeResolutionInFlight.get(cacheKey) === request) listingTimeResolutionInFlight.delete(cacheKey);
        }).catch(() => {});
    }

    const resolved = await waitForPromiseWithAbortSignal(request, signal);
    const listingTime = persistResolvedListingTime(token, resolved?.listingTime, resolved?.source);
    return { listingTime, source: String(resolved?.source || (listingTime > 0 ? "resolved" : "unresolved")) };
}

function hasInitialKlinePageReachedDataStart(candles, requestedLimit, token, timeframe) {
    if (isTradeOnlyTimeframe(timeframe) || !Array.isArray(candles) || candles.length === 0) return false;
    const market = getTokenMarket(token);
    if (market !== MARKET.SPOT && market !== MARKET.FUTURES) return false;
    const safeLimit = Math.max(1, Math.min(CHART_MAX_CANDLES, Number(requestedLimit) || CHART_MAX_CANDLES));
    return candles.length < safeLimit;
}

function hasReachedTokenListingBoundary(candles, token, timeframe) {
    if (!Array.isArray(candles) || candles.length === 0 || !token) return false;

    const listingTime = getTokenListingTimeMs(token);
    if (listingTime <= 0) return false;

    const listingBucketStart = getTimeframeBucketStartMs(listingTime, timeframe) || listingTime;
    const listingBoundaryEnd = getNextTimeframeBucketStartMs(listingBucketStart, timeframe);
    const firstCandleTimeSeconds = Number(candles[0]?.time);
    const lastCandleTimeSeconds = Number(candles[candles.length - 1]?.time);
    if (!Number.isFinite(firstCandleTimeSeconds) || firstCandleTimeSeconds <= 0) return false;
    if (!Number.isFinite(lastCandleTimeSeconds) || lastCandleTimeSeconds <= 0) return false;

    const firstCandleTimeMs = Math.floor(firstCandleTimeSeconds * 1000);
    const lastCandleTimeMs = Math.floor(lastCandleTimeSeconds * 1000);
    const safeBoundaryEnd = Number.isFinite(listingBoundaryEnd) && listingBoundaryEnd > listingBucketStart
        ? listingBoundaryEnd
        : listingBucketStart;

    return firstCandleTimeMs <= safeBoundaryEnd
        && lastCandleTimeMs >= listingBucketStart;
}

function getTimeframeBucketStartMs(timeMs, timeframe) {
    const numeric = Number(timeMs);
    if (!Number.isFinite(numeric) || numeric <= 0) return 0;
    if (timeframe === "1M") {
        const date = new Date(numeric);
        return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
    }
    const seconds = getTimeframeSeconds(timeframe);
    if (seconds <= 0) return 0;
    const intervalMs = seconds * 1000;
    return Math.floor(numeric / intervalMs) * intervalMs;
}

function getNextTimeframeBucketStartMs(bucketStartMs, timeframe) {
    const numeric = Number(bucketStartMs);
    if (!Number.isFinite(numeric) || numeric <= 0) return 0;
    if (timeframe === "1M") {
        const date = new Date(numeric);
        return Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1);
    }
    const intervalMs = Math.max(1000, getTimeframeSeconds(timeframe) * 1000);
    return numeric + intervalMs;
}

function getEstimatedTimeframeIntervalMs(timeframe) {
    if (timeframe === "1M") return 30 * 24 * 60 * 60 * 1000;
    return Math.max(1000, getTimeframeSeconds(timeframe) * 1000);
}

function getKlinesUrl(token, timeframe, limit, startTime = null, endTime = null) {
    const config = getMarketConfig(token);
    const symbol = encodeURIComponent(getRequestSymbol(token));
    let url = `${config.klinesUrl}?symbol=${symbol}&interval=${encodeURIComponent(timeframe)}&limit=${Math.max(1, Math.min(CHART_MAX_CANDLES, Number(limit) || CHART_MAX_CANDLES))}`;
    const normalizedStart = normalizeEpochMilliseconds(startTime);
    const normalizedEnd = normalizeEpochMilliseconds(endTime);
    if (normalizedStart > 0) url += `&startTime=${normalizedStart}`;
    if (normalizedEnd > 0) url += `&endTime=${normalizedEnd}`;
    return url;
}

function mapChartCandleData(raw) {
    if (!Array.isArray(raw)) return [];
    const mapped = [];
    for (const row of raw) {
        if (!Array.isArray(row) || row.length < 6) continue;
        const time = Math.floor(Number(row[0]) / 1000);
        const open = Number.parseFloat(row[1]);
        const high = Number.parseFloat(row[2]);
        const low = Number.parseFloat(row[3]);
        const close = Number.parseFloat(row[4]);
        const volume = Number.parseFloat(row[5]);
        const takerBuyVolume = Number.parseFloat(row[9]);
        if (![time, open, high, low, close].every(Number.isFinite)) continue;
        mapped.push({
            time,
            open,
            high,
            low,
            close,
            volume: Number.isFinite(volume) ? volume : 0,
            takerBuyVolume: Number.isFinite(takerBuyVolume) ? takerBuyVolume : 0
        });
    }
    return mapped.sort((a, b) => a.time - b.time);
}

function fillOneSecondCandleGaps(candles, options = {}) {
    const currentOpenTime = Math.max(0, Math.floor(Number(options.currentOpenTime) || 0));
    const source = (Array.isArray(candles) ? candles : [])
        .filter(candle => candle && Number.isFinite(Number(candle.time)))
        .map(candle => ({
            ...candle,
            time: Math.floor(Number(candle.time)),
            open: Number(candle.open),
            high: Number(candle.high),
            low: Number(candle.low),
            close: Number(candle.close),
            volume: Number.isFinite(Number(candle.volume)) ? Number(candle.volume) : 0,
            takerBuyVolume: Number.isFinite(Number(candle.takerBuyVolume)) ? Number(candle.takerBuyVolume) : 0
        }))
        .filter(candle => [candle.time, candle.open, candle.high, candle.low, candle.close].every(Number.isFinite))
        .filter(candle => currentOpenTime <= 0 || candle.time < currentOpenTime)
        .sort((left, right) => left.time - right.time);
    if (source.length === 0) return [];

    const unique = [];
    for (const candle of source) {
        const previous = unique[unique.length - 1];
        if (previous?.time === candle.time) unique[unique.length - 1] = candle;
        else unique.push(candle);
    }

    const lastTime = unique[unique.length - 1].time;
    const minimumTime = Math.max(unique[0].time, lastTime - CHART_MAX_CANDLES + 1);
    let seed = unique[0];
    for (const candle of unique) {
        if (candle.time > minimumTime) break;
        seed = candle;
    }
    const filled = [];
    let previousClose = seed.close;
    let sourceIndex = unique.findIndex(candle => candle.time >= minimumTime);
    if (sourceIndex < 0) sourceIndex = unique.length - 1;
    for (let time = minimumTime; time <= lastTime; time += 1) {
        const sourceCandle = unique[sourceIndex];
        if (sourceCandle && sourceCandle.time === time) {
            filled.push(sourceCandle);
            previousClose = sourceCandle.close;
            sourceIndex += 1;
        } else {
            filled.push({ time, open: previousClose, high: previousClose, low: previousClose, close: previousClose, volume: 0, takerBuyVolume: 0 });
        }
    }
    return filled;
}

function mapAggregateTradesToOneSecondCandles(rawTrades) {
    if (!Array.isArray(rawTrades) || rawTrades.length === 0) return [];
    const byTime = new Map();
    const sorted = rawTrades.slice().sort((left, right) => {
        const timeDifference = (Number(left?.T || left?.E) || 0) - (Number(right?.T || right?.E) || 0);
        if (timeDifference !== 0) return timeDifference;
        return (Number(left?.a) || 0) - (Number(right?.a) || 0);
    });
    for (const trade of sorted) {
        const tradeTime = Number(trade?.T || trade?.E);
        const tradeId = Number(trade?.a);
        const price = Number(trade?.p);
        const qty = Number(trade?.q);
        if (!Number.isFinite(tradeTime) || tradeTime <= 0 || !Number.isFinite(price) || price <= 0 || !Number.isFinite(qty) || qty <= 0) continue;
        const time = Math.floor(tradeTime / 1000);
        let candle = byTime.get(time);
        if (!candle) {
            candle = {
                time,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 0,
                takerBuyVolume: 0,
                firstTradeTime: tradeTime,
                lastTradeTime: tradeTime,
                firstTradeId: Number.isFinite(tradeId) ? tradeId : 0,
                lastTradeId: Number.isFinite(tradeId) ? tradeId : 0
            };
            byTime.set(time, candle);
        }
        const isEarlier = tradeTime < candle.firstTradeTime
            || (tradeTime === candle.firstTradeTime && Number.isFinite(tradeId) && tradeId < candle.firstTradeId);
        const isLater = tradeTime > candle.lastTradeTime
            || (tradeTime === candle.lastTradeTime && Number.isFinite(tradeId) && tradeId > candle.lastTradeId);
        if (isEarlier) {
            candle.open = price;
            candle.firstTradeTime = tradeTime;
            candle.firstTradeId = Number.isFinite(tradeId) ? tradeId : candle.firstTradeId;
        }
        if (isLater) {
            candle.close = price;
            candle.lastTradeTime = tradeTime;
            candle.lastTradeId = Number.isFinite(tradeId) ? tradeId : candle.lastTradeId;
        }
        candle.high = Math.max(candle.high, price);
        candle.low = Math.min(candle.low, price);
        candle.volume += qty;
        if (normalizeAggTradeBuyerMakerFlag(trade?.m) === false) candle.takerBuyVolume += qty;
    }
    return fillOneSecondCandleGaps(Array.from(byTime.values()));
}

const futuresOneSecondInitialHistoryInFlight = new Map();
const futuresOneSecondInitialLastTradeIdBySymbol = new Map();

function createAbortError(signal, fallbackMessage = "Aborted") {
    return signal?.reason instanceof Error
        ? signal.reason
        : new DOMException(fallbackMessage, "AbortError");
}

function waitForPromiseWithAbortSignal(promise, signal) {
    if (!signal) return promise;
    if (signal.aborted) return Promise.reject(createAbortError(signal));
    return new Promise((resolve, reject) => {
        const cleanup = () => signal.removeEventListener("abort", handleAbort);
        const handleAbort = () => {
            cleanup();
            reject(createAbortError(signal));
        };
        signal.addEventListener("abort", handleAbort, { once: true });
        promise.then(
            value => {
                cleanup();
                resolve(value);
            },
            error => {
                cleanup();
                reject(error);
            }
        );
    });
}

const oneSecondReconcileTailCache = new Map();

async function fetchOneSecondReconcileTail(token, limit, signal) {
    const safeLimit = Math.max(2, Math.min(100, Number(limit) || ONE_SECOND_RECONCILE_FETCH_LIMIT));
    const key = `${getTokenKey(token)}:${safeLimit}`;
    const now = nowMs();
    let entry = oneSecondReconcileTailCache.get(key);
    if (!entry || entry.expiresAt <= now) {
        const controller = new AbortController();
        entry = {
            expiresAt: now + ONE_SECOND_RECONCILE_CACHE_TTL_MS,
            promise: fetchCandlesForChart(token, "1s", safeLimit, controller.signal)
                .finally(() => {
                    const current = oneSecondReconcileTailCache.get(key);
                    if (current === entry) current.expiresAt = nowMs() + ONE_SECOND_RECONCILE_CACHE_TTL_MS;
                })
        };
        oneSecondReconcileTailCache.set(key, entry);
    }
    const candles = await waitForPromiseWithAbortSignal(entry.promise, signal);
    return candles.map(candle => ({ ...candle }));
}

function rawChartCandleEquals(left, right) {
    if (!left || !right || Number(left.time) !== Number(right.time)) return false;
    return Number(left.open) === Number(right.open)
        && Number(left.high) === Number(right.high)
        && Number(left.low) === Number(right.low)
        && Number(left.close) === Number(right.close)
        && Number(left.volume || 0) === Number(right.volume || 0)
        && Number(left.takerBuyVolume || 0) === Number(right.takerBuyVolume || 0);
}

function mergeOneSecondReconcileCandles(rawBase, authoritativeTail, protectedOpenTime) {
    const merged = rawBase.map(candle => ({ ...candle }));
    const indexByTime = new Map();
    for (let index = 0; index < merged.length; index += 1) indexByTime.set(merged[index].time, index);
    let replaceStartIndex = merged.length;
    let correctedCount = 0;
    for (const candle of authoritativeTail) {
        const time = Math.floor(Number(candle?.time));
        if (!Number.isFinite(time) || time <= 0 || (protectedOpenTime > 0 && time >= protectedOpenTime)) continue;
        const index = indexByTime.get(time);
        if (index === undefined || rawChartCandleEquals(merged[index], candle)) continue;
        merged[index] = {
            time,
            open: Number(candle.open),
            high: Number(candle.high),
            low: Number(candle.low),
            close: Number(candle.close),
            volume: Number.isFinite(Number(candle.volume)) ? Number(candle.volume) : 0,
            takerBuyVolume: Number.isFinite(Number(candle.takerBuyVolume)) ? Number(candle.takerBuyVolume) : 0
        };
        replaceStartIndex = Math.min(replaceStartIndex, index);
        correctedCount += 1;
    }
    return { merged, replaceStartIndex, correctedCount };
}

async function fetchFuturesOneSecondInitialHistory(token, signal) {
    const config = getMarketConfig(MARKET.FUTURES);
    if (!config.aggTradesUrl) throw new Error("Missing Futures aggregate-trades endpoint");
    const symbol = encodeURIComponent(getRequestSymbol(token));

    const recentUrl = `${config.aggTradesUrl}?symbol=${symbol}&limit=1000`;
    const recentTrades = await fetchJsonWithTimeout(recentUrl, { signal }, 10000);
    let allTrades = Array.isArray(recentTrades) ? recentTrades : [];

    if (allTrades.length > 0 && !signal?.aborted) {
        const firstTradeId = Number(allTrades[0]?.a);
        if (Number.isInteger(firstTradeId) && firstTradeId > 1) {
            const startId = Math.max(1, firstTradeId - 1000);
            const previousUrl = `${config.aggTradesUrl}?symbol=${symbol}&fromId=${startId}&limit=1000`;
            try {
                const previousTrades = await fetchJsonWithTimeout(previousUrl, { signal }, 10000);
                if (Array.isArray(previousTrades) && previousTrades.length > 0) {
                    const seenIds = new Set();
                    const mergedTrades = [];
                    for (const trade of previousTrades) {
                        if (!trade || trade.a === undefined || seenIds.has(trade.a)) continue;
                        seenIds.add(trade.a);
                        mergedTrades.push(trade);
                    }
                    for (const trade of allTrades) {
                        if (!trade || trade.a === undefined || seenIds.has(trade.a)) continue;
                        seenIds.add(trade.a);
                        mergedTrades.push(trade);
                    }
                    allTrades = mergedTrades;
                }
            } catch (error) {
                if (signal?.aborted || error?.name === "AbortError") throw error;
                console.warn("Failed to fetch second batch of historical aggTrades:", error);
            }
        }
    }

    if (signal?.aborted) throw createAbortError(signal);
    let lastAggregateTradeId = 0;
    for (const trade of allTrades) {
        const tradeId = Number(trade?.a);
        if (Number.isInteger(tradeId) && tradeId > lastAggregateTradeId) lastAggregateTradeId = tradeId;
    }
    futuresOneSecondInitialLastTradeIdBySymbol.set(getRequestSymbol(token), lastAggregateTradeId);
    const tokenKey = getTokenKey(token);
    if (lastAggregateTradeId > (Number(lastAggregateTradeIdByToken.get(tokenKey)) || 0)) {
        lastAggregateTradeIdByToken.set(tokenKey, lastAggregateTradeId);
    }
    return mapAggregateTradesToOneSecondCandles(allTrades).slice(-CHART_MAX_CANDLES);
}

function getOrCreateFuturesOneSecondInitialHistoryRequest(token) {
    const requestSymbol = getRequestSymbol(token);
    let entry = futuresOneSecondInitialHistoryInFlight.get(requestSymbol);
    if (entry) return entry;

    const controller = new AbortController();
    entry = {
        requestSymbol,
        controller,
        consumers: 0,
        settled: false,
        promise: null
    };
    entry.promise = fetchFuturesOneSecondInitialHistory(token, controller.signal);
    futuresOneSecondInitialHistoryInFlight.set(requestSymbol, entry);
    void entry.promise.finally(() => {
        entry.settled = true;
        if (futuresOneSecondInitialHistoryInFlight.get(requestSymbol) === entry) {
            futuresOneSecondInitialHistoryInFlight.delete(requestSymbol);
        }
    }).catch(() => {});
    return entry;
}

async function fetchFuturesOneSecondCandles(token, limit, signal) {
    if (signal?.aborted) throw createAbortError(signal);

    const entry = getOrCreateFuturesOneSecondInitialHistoryRequest(token);
    entry.consumers += 1;
    try {
        const candles = await waitForPromiseWithAbortSignal(entry.promise, signal);
        if (signal?.aborted) throw createAbortError(signal);
        const targetCandles = Math.max(1, Math.min(CHART_MAX_CANDLES, Number(limit) || CHART_MAX_CANDLES));
        return candles.slice(-targetCandles).map(candle => ({ ...candle }));
    } finally {
        entry.consumers = Math.max(0, entry.consumers - 1);
        if (entry.consumers === 0 && !entry.settled && !entry.controller.signal.aborted) {
            if (futuresOneSecondInitialHistoryInFlight.get(entry.requestSymbol) === entry) {
                futuresOneSecondInitialHistoryInFlight.delete(entry.requestSymbol);
            }
            entry.controller.abort(new DOMException("No active Futures 1s history consumers", "AbortError"));
        }
    }
}

function mergeChartCandles(existing, incoming) {
    const byTime = new Map();
    for (const candle of existing || []) {
        if (candle && Number.isFinite(candle.time)) byTime.set(candle.time, { ...candle });
    }
    for (const candle of incoming || []) {
        if (candle && Number.isFinite(candle.time)) byTime.set(candle.time, { ...candle });
    }
    return Array.from(byTime.values()).sort((a, b) => a.time - b.time).slice(-CHART_MAX_CANDLES);
}

function applyChartWMA(candles, period, sourceKey, targetKey) {
    const n = candles.length;
    if (!Number.isFinite(period) || period <= 0 || n < period) return;
    const weightSum = (period * (period + 1)) / 2;
    let currentTotalSum = 0;
    let currentWeightedSum = 0;
    let validStartIndex = -1;
    for (let i = 0; i < n; i += 1) {
        if (candles[i][sourceKey] !== undefined && candles[i][sourceKey] !== null && Number.isFinite(candles[i][sourceKey])) {
            validStartIndex = i;
            break;
        }
    }
    if (validStartIndex === -1 || n - validStartIndex < period) return;
    for (let i = 0; i < period; i += 1) {
        const value = candles[validStartIndex + i][sourceKey];
        currentTotalSum += value;
        currentWeightedSum += value * (i + 1);
    }
    candles[validStartIndex + period - 1][targetKey] = currentWeightedSum / weightSum;
    for (let i = validStartIndex + period; i < n; i += 1) {
        const newPrice = candles[i][sourceKey];
        const oldPrice = candles[i - period][sourceKey];
        currentWeightedSum = currentWeightedSum - currentTotalSum + newPrice * period;
        currentTotalSum = currentTotalSum - oldPrice + newPrice;
        candles[i][targetKey] = currentWeightedSum / weightSum;
    }
}

function applyChartHMA(candles, period, sourceKey, targetKey) {
    const safePeriod = Math.max(1, Math.round(period || 1));
    const nHalf = Math.max(1, Math.floor(safePeriod / 2));
    const nSqrt = Math.max(1, Math.floor(Math.sqrt(safePeriod)));
    const keyHalf = `_wmaHalf_${targetKey}_${safePeriod}`;
    const keyFull = `_wmaFull_${targetKey}_${safePeriod}`;
    const keyDiff = `_hmaDiff_${targetKey}_${safePeriod}`;
    applyChartWMA(candles, nHalf, sourceKey, keyHalf);
    applyChartWMA(candles, safePeriod, sourceKey, keyFull);
    for (const candle of candles) {
        const half = candle[keyHalf];
        const full = candle[keyFull];
        if (Number.isFinite(half) && Number.isFinite(full)) candle[keyDiff] = (2 * half) - full;
    }
    applyChartWMA(candles, nSqrt, keyDiff, targetKey);
}

function applyChartDeviationCloud(candles, emaLength, basisMode) {
    const safeLength = Math.max(1, Math.round(emaLength || 1));
    if (candles.length < safeLength * 2) return;
    const stDevPeriod = safeLength * 2;
    const R1 = 0.92, R2 = 2.0, L4 = 3.8, L5 = 5.5, L6 = 6.0, L8 = 8.0;

    if (basisMode === "vwap") {
        let cumPV = 0;
        let cumVol = 0;
        for (let i = 0; i < candles.length; i += 1) {
            const candle = candles[i];
            const typical = (candle.high + candle.low + candle.close) / 3;
            const volume = candle.volume || 0;
            cumPV += typical * volume;
            cumVol += volume;
            if (i >= safeLength) {
                const old = candles[i - safeLength];
                const oldTypical = (old.high + old.low + old.close) / 3;
                cumPV -= oldTypical * (old.volume || 0);
                cumVol -= old.volume || 0;
            }
            candle.threeEma = cumVol > 0 ? cumPV / cumVol : candle.close;
        }
    } else {
        const k = 2 / (safeLength + 1);
        let currentEma = null;
        for (const candle of candles) {
            currentEma = currentEma === null ? candle.close : (candle.close - currentEma) * k + currentEma;
            candle.threeEma = currentEma;
        }
    }

    let sum = 0;
    let sumSq = 0;
    for (let i = 0; i < candles.length; i += 1) {
        const candle = candles[i];
        sum += candle.close;
        sumSq += candle.close * candle.close;
        if (i >= stDevPeriod) {
            const oldClose = candles[i - stDevPeriod].close;
            sum -= oldClose;
            sumSq -= oldClose * oldClose;
        }
        if (i >= stDevPeriod - 1) {
            const mean = sum / stDevPeriod;
            const variance = Math.max(0, (sumSq / stDevPeriod) - mean * mean);
            const stdev = Math.sqrt(variance);
            const cloudSize = stdev / 4;
            if (cloudSize <= 0 || !Number.isFinite(candle.threeEma)) continue;
            const base = candle.threeEma;
            candle.devCloudSize = cloudSize;
            candle.devL1 = base - cloudSize * R1; candle.devH1 = base + cloudSize * R1;
            candle.devL2 = base - cloudSize * R2; candle.devH2 = base + cloudSize * R2;
            candle.devL4 = base - cloudSize * L4; candle.devH4 = base + cloudSize * L4;
            candle.devL5 = base - cloudSize * L5; candle.devH5 = base + cloudSize * L5;
            candle.devL6 = base - cloudSize * L6; candle.devH6 = base + cloudSize * L6;
            candle.devL8 = base - cloudSize * L8; candle.devH8 = base + cloudSize * L8;
        }
    }
}

function isDifEnabledForChartCount(difConfig, chartCount = 4) {
    const config = difConfig && typeof difConfig === "object" ? difConfig : {};
    const normalizedCount = Number(chartCount) === 6 ? 6 : 4;
    const enabledMap = config.enabledByChartCount && typeof config.enabledByChartCount === "object"
        ? config.enabledByChartCount
        : null;
    if (enabledMap) return (enabledMap[normalizedCount] ?? enabledMap[String(normalizedCount)]) === true;
    return config.enabled === true;
}

function resolveChartIndicatorRuntimeState(settings = state.settings.charts || DEFAULT_CHART_SETTINGS, timeframe = null) {
    const cfg = settings?.indicators || DEFAULT_CHART_SETTINGS.indicators;
    const performance = settings?.performance || DEFAULT_CHART_SETTINGS.performance || {};
    const enabled = key => cfg?.[key]?.enabled === true;
    const isOneSecond = timeframe === "1s";
    const lightweight1s = isOneSecond && performance.lightweight1s !== false;
    const v69Compatible1s = isOneSecond && performance.v69Compatible1s !== false;
    const effectiveDifMode = lightweight1s ? "basic" : (cfg?.dif?.renderMode || "standard");
    const difActive = isDifEnabledForChartCount(cfg?.dif, settings?.chartCount)
        && (timeframe !== "1s" || cfg?.dif?.enabledOn1s !== false);
    const hma35 = enabled("hma35");
    const hma55 = enabled("hma55");
    const hma100 = enabled("hma100");
    const hma80 = hma55 || hma100;
    const supportsSmaHma55Cross = timeframe === "1s" || timeframe === "1m";
    const smaCrossMarkersActive = cfg?.sma?.crossMarkersEnabled !== false
        && (hma35 || (supportsSmaHma55Cross && hma55));
    const smaCrossSoundActive = cfg?.sma?.crossSoundEnabled === true
        && supportsSmaHma55Cross
        && hma55;
    const smaDataRequired = enabled("sma") || smaCrossMarkersActive || smaCrossSoundActive;
    // V80: lightweight/V69-compatible режим не вимикає самі HMA/devCloud fill primitives.
    // Він керує лише частотою live-tail оновлення на UI-рівні.
    const hmaFillActive = hma55 || hma100;
    const devFillActive = enabled("devCloud");
    const flags = {
        volume: enabled("volume"),
        sma: enabled("sma"),
        bb: enabled("bb"),
        bb2: enabled("bb2") && timeframe === "1s",
        hma25: enabled("hma25"),
        hma35,
        hma55,
        hma80,
        hma100,
        devCloud: enabled("devCloud"),
        dif: difActive
    };
    return {
        timeframe,
        isOneSecond,
        lightweight1s,
        v69Compatible1s,
        tailFillThrottleMs: Math.max(100, Math.min(2000, Math.round(Number(performance.tailFillThrottleMs) || DEFAULT_CHART_SETTINGS.performance.tailFillThrottleMs || 750))),
        effectiveDifMode,
        difActive,
        smaCrossMarkersActive,
        smaCrossSoundActive,
        smaDataRequired,
        hmaFillActive,
        devFillActive,
        allowTailFillUpdate: !isOneSecond || (!lightweight1s && !v69Compatible1s),
        flags
    };
}

function getEnabledChartIndicatorFlags(settings = state.settings.charts || DEFAULT_CHART_SETTINGS, timeframe = null) {
    return resolveChartIndicatorRuntimeState(settings, timeframe).flags;
}

function buildChartTransferFields(runtimeState, includeDif = false) {
    const runtime = runtimeState || resolveChartIndicatorRuntimeState();
    const fields = [...CHART_CANDLE_REQUIRED_TRANSFER_FIELDS];
    const flags = runtime.flags || {};
    if (runtime.smaDataRequired) appendUniqueFields(fields, CHART_CANDLE_PRICE_FIELD_GROUPS.sma);
    if (flags.bb) appendUniqueFields(fields, CHART_CANDLE_PRICE_FIELD_GROUPS.bb);
    if (flags.bb2) appendUniqueFields(fields, CHART_CANDLE_PRICE_FIELD_GROUPS.bb2);
    if (flags.hma25) appendUniqueFields(fields, CHART_CANDLE_PRICE_FIELD_GROUPS.hma25);
    if (flags.hma35 || runtime.hmaFillActive) appendUniqueFields(fields, CHART_CANDLE_PRICE_FIELD_GROUPS.hma35);
    if (flags.hma55 || runtime.hmaFillActive) appendUniqueFields(fields, CHART_CANDLE_PRICE_FIELD_GROUPS.hma55);
    if (flags.hma80 || runtime.hmaFillActive) appendUniqueFields(fields, CHART_CANDLE_PRICE_FIELD_GROUPS.hma80);
    if (flags.hma100 || runtime.hmaFillActive) appendUniqueFields(fields, CHART_CANDLE_PRICE_FIELD_GROUPS.hma100);
    if (flags.devCloud) appendUniqueFields(fields, CHART_CANDLE_PRICE_FIELD_GROUPS.devCloud);
    if (state.settings.scalping?.enabled === true) appendUniqueFields(fields, CHART_CANDLE_PRICE_FIELD_GROUPS.cvd);
    if (includeDif && runtime.difActive) appendUniqueFields(fields, CHART_CANDLE_DIF_TRANSFER_FIELDS);
    return fields;
}

function buildIndicatorTransferFields(runtimeState, includeDif = false) {
    return buildChartTransferFields(runtimeState, includeDif)
        .filter(field => field === "time" || !CHART_CANDLE_RAW_TRANSFER_FIELD_SET.has(field));
}

function shouldIncludeDifFields(settings = state.settings.charts || DEFAULT_CHART_SETTINGS, timeframe = null) {
    return resolveChartIndicatorRuntimeState(settings, timeframe).difActive === true;
}

function getScalpingIndicatorWarmupBars() {
    const scalping = state.settings.scalping || {};
    if (scalping.enabled !== true || scalping.cvdEnabled !== true) return 0;
    if (scalping.cvdAdvancedEnabled !== true || scalping.cvdRollingEnabled !== true) {
        return CHART_MAX_CANDLES;
    }

    const rollingWindow = Math.max(10, Math.min(2000, Math.round(Number(scalping.cvdRollingWindow) || 200)));
    const bandsPeriod = Math.max(5, Math.min(500, Math.round(Number(scalping.cvdBandsPeriod) || 20)));
    const statisticsWindow = Math.max(30, Math.min(200, Math.round(bandsPeriod * 2.5)));
    const levelLookback = Math.max(12, Math.min(120, Math.round(rollingWindow * 0.15)));
    return Math.min(
        CHART_MAX_CANDLES,
        Math.max(rollingWindow, bandsPeriod, statisticsWindow, levelLookback) + CHART_INDICATOR_INCREMENTAL_EXTRA_BARS
    );
}

function getChartIndicatorWarmupBars(settings = state.settings.charts || DEFAULT_CHART_SETTINGS, timeframe = null) {
    const cfg = settings?.indicators || DEFAULT_CHART_SETTINGS.indicators;
    const runtime = resolveChartIndicatorRuntimeState(settings, timeframe);
    const flags = runtime.flags;
    const lengthOf = (key, fallback) => Math.max(1, Math.round(Number(cfg?.[key]?.length) || fallback));
    let warmup = 0;

    if (runtime.smaDataRequired) {
        warmup = Math.max(warmup, lengthOf("sma", DEFAULT_CHART_SETTINGS.indicators.sma.length));
    }
    if (flags.bb) {
        warmup = Math.max(warmup, lengthOf("bb", DEFAULT_CHART_SETTINGS.indicators.bb.length));
    }
    if (flags.bb2) {
        warmup = Math.max(warmup, lengthOf("bb2", DEFAULT_CHART_SETTINGS.indicators.bb2.length));
    }
    if (flags.dif) {
        const hmLen = Math.max(1, Math.round(Number(cfg?.dif?.hmLength) || 60));
        const rsiLen = Math.max(1, Math.round(Number(cfg?.dif?.rsiLength) || 9));
        const hmaWarmup = hmLen + Math.ceil(Math.sqrt(hmLen));
        warmup = Math.max(warmup, rsiLen * 2 + hmaWarmup);
    }

    const addHmaWarmup = (key, fallback) => {
        const length = lengthOf(key, fallback);
        warmup = Math.max(warmup, length + Math.ceil(Math.sqrt(length)));
    };
    if (flags.hma25) addHmaWarmup("hma25", DEFAULT_CHART_SETTINGS.indicators.hma25.length);
    if (flags.hma35) addHmaWarmup("hma35", DEFAULT_CHART_SETTINGS.indicators.hma35.length);
    if (flags.hma55) addHmaWarmup("hma55", DEFAULT_CHART_SETTINGS.indicators.hma55.length);
    if (flags.hma80) warmup = Math.max(warmup, 80 + Math.ceil(Math.sqrt(80)));
    if (flags.hma100) addHmaWarmup("hma100", DEFAULT_CHART_SETTINGS.indicators.hma100.length);

    if (flags.devCloud) {
        const length = Math.max(1, Math.round(Number(cfg?.devCloud?.emaLength) || DEFAULT_CHART_SETTINGS.indicators.devCloud.emaLength));
        warmup = Math.max(warmup, length * 2);
    }

    if (warmup <= 0) return 0;
    return Math.min(CHART_MAX_CANDLES, Math.ceil(warmup) + CHART_INDICATOR_INCREMENTAL_EXTRA_BARS);
}

function applyChartEMA(candles, period, sourceKey, targetKey) {
    if (!candles || candles.length === 0) return;
    const k = 2 / (period + 1);
    let ema = null;
    for (let i = 0; i < candles.length; i += 1) {
        const val = candles[i][sourceKey];
        if (Number.isFinite(val)) {
            ema = ema === null ? val : (val - ema) * k + ema;
            candles[i][targetKey] = ema;
        } else {
            candles[i][targetKey] = NaN;
        }
    }
}

function applyChartSMA(candles, period, sourceKey, targetKey) {
    if (!candles || candles.length === 0) return;
    let sum = 0;
    let count = 0;
    for (let i = 0; i < candles.length; i += 1) {
        const val = candles[i][sourceKey];
        if (Number.isFinite(val)) {
            sum += val;
            count += 1;
        }
        if (i >= period) {
            const oldVal = candles[i - period][sourceKey];
            if (Number.isFinite(oldVal)) {
                sum -= oldVal;
                count -= 1;
            }
        }
        if (i >= period - 1) {
            candles[i][targetKey] = count > 0 ? sum / count : NaN;
        } else {
            candles[i][targetKey] = NaN;
        }
    }
}

function getWMAArray(srcArray, period, outArray) {
    const n = srcArray.length;
    outArray.fill(NaN);
    if (!Number.isFinite(period) || period <= 0 || n < period) return;
    const weightSum = (period * (period + 1)) / 2;
    
    let validStartIndex = -1;
    for (let i = 0; i < n; i += 1) {
        if (!Number.isNaN(srcArray[i])) {
            validStartIndex = i;
            break;
        }
    }
    if (validStartIndex === -1 || n - validStartIndex < period) return;
    
    let currentTotalSum = 0;
    let currentWeightedSum = 0;
    for (let i = 0; i < period; i += 1) {
        const val = srcArray[validStartIndex + i];
        currentTotalSum += val;
        currentWeightedSum += val * (i + 1);
    }
    outArray[validStartIndex + period - 1] = currentWeightedSum / weightSum;
    
    const invWeightSum = 1 / weightSum;
    for (let i = validStartIndex + period; i < n; i += 1) {
        const newPrice = srcArray[i];
        const oldPrice = srcArray[i - period];
        currentWeightedSum = currentWeightedSum - currentTotalSum + newPrice * period;
        currentTotalSum = currentTotalSum - oldPrice + newPrice;
        outArray[i] = currentWeightedSum * invWeightSum;
    }
}

function getHMAArray(srcArray, period, outArray, temp1, temp2, temp3) {
    const safePeriod = Math.max(1, Math.round(period || 1));
    const nHalf = Math.max(1, Math.floor(safePeriod / 2));
    const nSqrt = Math.max(1, Math.floor(Math.sqrt(safePeriod)));
    const n = srcArray.length;
    
    getWMAArray(srcArray, nHalf, temp1);
    getWMAArray(srcArray, safePeriod, temp2);
    
    for (let i = 0; i < n; i += 1) {
        const half = temp1[i];
        const full = temp2[i];
        temp3[i] = (!Number.isNaN(half) && !Number.isNaN(full)) ? (2 * half) - full : NaN;
    }
    
    getWMAArray(temp3, nSqrt, outArray);
}

function getSMAArray(srcArray, period, outArray) {
    const n = srcArray.length;
    outArray.fill(NaN);
    if (!Number.isFinite(period) || period <= 0 || n < period) return;
    
    let sum = 0;
    let count = 0;
    for (let i = 0; i < n; i += 1) {
        const val = srcArray[i];
        if (!Number.isNaN(val)) {
            sum += val;
            count += 1;
        }
        if (i >= period) {
            const oldVal = srcArray[i - period];
            if (!Number.isNaN(oldVal)) {
                sum -= oldVal;
                count -= 1;
            }
        }
        if (i >= period - 1) {
            outArray[i] = count > 0 ? sum / count : NaN;
        }
    }
}

function getEMAArray(srcArray, period, outArray) {
    const n = srcArray.length;
    outArray.fill(NaN);
    if (!Number.isFinite(period) || period <= 0 || n === 0) return;
    const k = 2 / (period + 1);
    let ema = null;
    for (let i = 0; i < n; i += 1) {
        const val = srcArray[i];
        if (!Number.isNaN(val)) {
            ema = ema === null ? val : (val - ema) * k + ema;
            outArray[i] = ema;
        }
    }
}

function calculateChartIndicators(candles, settings = state.settings.charts || DEFAULT_CHART_SETTINGS, timeframe = null, token = state.selectedToken) {
    const cfg = settings.indicators || DEFAULT_CHART_SETTINGS.indicators;
    const runtime = resolveChartIndicatorRuntimeState(settings, timeframe);
    const flags = runtime.flags;
    const smaDataRequired = runtime.smaDataRequired === true;
    const includeCvdPlaceholders = state.settings.scalping?.enabled === true;
    const createOutputCandle = (candle) => {
        const out = {
            time: candle.time,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume || 0,
            takerBuyVolume: candle.takerBuyVolume || 0
        };
        if (smaDataRequired) out.sma7 = NaN;
        if (flags.bb) { out.bbUpper = NaN; out.bbMiddle = NaN; out.bbLower = NaN; }
        if (flags.bb2) { out.bb2Upper = NaN; out.bb2Middle = NaN; out.bb2Lower = NaN; }
        if (flags.hma25) out.hma25 = NaN;
        if (flags.hma35) out.hma35 = NaN;
        if (flags.hma55) out.hma55 = NaN;
        if (flags.hma80) out.hma80 = NaN;
        if (flags.hma100) out.hma100 = NaN;
        if (flags.devCloud) {
            out.threeEma = NaN;
            out.devCloudSize = NaN;
            out.devL1 = NaN; out.devH1 = NaN; out.devL2 = NaN; out.devH2 = NaN;
            out.devL4 = NaN; out.devH4 = NaN; out.devL5 = NaN; out.devH5 = NaN;
            out.devL6 = NaN; out.devH6 = NaN; out.devL8 = NaN; out.devH8 = NaN;
        }
        if (includeCvdPlaceholders) {
            out.cvd = null;
            out.cvdBBMiddle = null;
            out.cvdBBUpper = null;
            out.cvdBBLower = null;
            out.cvdDivSignal = null;
            out.cvdDivSignalCode = 0;
            out.cvdSignalVersion = 0;
            out.cvdSignalStrength = 0;
            out.cvdSignalConfidenceCode = 0;
            out.cvdSignalReasonCode = 0;
            out.cvdDeltaZ = 0;
            out.cvdVolumeZ = 0;
            out.cvdPriceReactionScore = 0;
            out.cvdLocationScore = 0;
            out.cvdBandScore = 0;
        }
        return out;
    };
    const data = (candles || []).map(createOutputCandle);
    if (!data.length) return data;
    const n = data.length;

    const smaPeriod = Math.max(1, Math.round(cfg.sma.length || DEFAULT_CHART_SETTINGS.indicators.sma.length));
    const bbPeriod = Math.max(1, Math.round(cfg.bb.length || DEFAULT_CHART_SETTINGS.indicators.bb.length));
    const bbMult = Number.isFinite(Number(cfg.bb.multiplier)) ? Number(cfg.bb.multiplier) : DEFAULT_CHART_SETTINGS.indicators.bb.multiplier;

    const bb2Period = Math.max(1, Math.round(cfg.bb2.length || DEFAULT_CHART_SETTINGS.indicators.bb2.length));
    const bb2Mult = Number.isFinite(Number(cfg.bb2.multiplier)) ? Number(cfg.bb2.multiplier) : DEFAULT_CHART_SETTINGS.indicators.bb2.multiplier;

    // =========================================================================
    // ОПТИМІЗОВАНИЙ БЛОК РОЗРАХУНКУ SMA, BB1 ТА BB2 (ЛІНІЙНА СКЛАДНІСТЬ O(N))
    // =========================================================================

    // 1. Ковзний розрахунок SMA (Simple Moving Average)
    if (smaDataRequired) {
        let runningSmaSum = 0;
        for (let i = 0; i < data.length; i += 1) {
            runningSmaSum += data[i].close;
            if (i >= smaPeriod) {
                runningSmaSum -= data[i - smaPeriod].close;
            }
            if (i >= smaPeriod - 1) {
                data[i].sma7 = runningSmaSum / smaPeriod;
            }
        }
    }

    // 2. Ковзний розрахунок Bollinger Bands 1
    if (flags.bb) {
        const p = bbPeriod;
        const mult = bbMult;
        let sumDiff = 0;
        let sumSqDiff = 0;
        const anchor = data[0].close; // "Якір" захищає від похибки округлення float

        for (let i = 0; i < data.length; i += 1) {
            const diff = data[i].close - anchor;
            sumDiff += diff;
            sumSqDiff += diff * diff;

            if (i >= p) {
                const oldDiff = data[i - p].close - anchor;
                sumDiff -= oldDiff;
                sumSqDiff -= oldDiff * oldDiff;
            }

            if (i >= p - 1) {
                const variance = (sumSqDiff - (sumDiff * sumDiff) / p) / p;
                const stdDev = Math.sqrt(Math.max(0, variance));
                const mean = (sumDiff / p) + anchor;

                data[i].bbMiddle = mean;
                data[i].bbUpper = mean + stdDev * mult;
                data[i].bbLower = mean - stdDev * mult;
            }
        }
    }

    // 3. Ковзний розрахунок Bollinger Bands 2 (Тільки для таймфрейму 1s)
    if (flags.bb2) {
        const p = bb2Period;
        const mult = bb2Mult;
        let sumDiff = 0;
        let sumSqDiff = 0;
        const anchor = data[0].close; // "Якір" захищає від похибки округлення float

        for (let i = 0; i < data.length; i += 1) {
            const diff = data[i].close - anchor;
            sumDiff += diff;
            sumSqDiff += diff * diff;

            if (i >= p) {
                const oldDiff = data[i - p].close - anchor;
                sumDiff -= oldDiff;
                sumSqDiff -= oldDiff * oldDiff;
            }

            if (i >= p - 1) {
                const variance = (sumSqDiff - (sumDiff * sumDiff) / p) / p;
                const stdDev = Math.sqrt(Math.max(0, variance));
                const mean = (sumDiff / p) + anchor;

                data[i].bb2Middle = mean;
                data[i].bb2Upper = mean + stdDev * mult;
                data[i].bb2Lower = mean - stdDev * mult;
            }
        }
    }
    // =========================================================================

    // Extract close and volume for optimized vectorized indicator math
    const closeArray = getReusableFloat64Buffer("close", n);
    const volumeArray = getReusableFloat64Buffer("volume", n);
    for (let i = 0; i < n; i += 1) {
        closeArray[i] = data[i].close;
        volumeArray[i] = data[i].volume || 0;
    }

    // Allocate reusable local buffers for HMA, EMA, SMA calculations
    const temp1 = getReusableFloat64Buffer("temp1", n);
    const temp2 = getReusableFloat64Buffer("temp2", n);
    const temp3 = getReusableFloat64Buffer("temp3", n);
    const outHMA = getReusableFloat64Buffer("outHMA", n);

    if (flags.hma25) {
        getHMAArray(closeArray, cfg.hma25.length, outHMA, temp1, temp2, temp3);
        for (let i = 0; i < n; i += 1) data[i].hma25 = outHMA[i];
    }
    if (flags.hma35) {
        getHMAArray(closeArray, cfg.hma35.length, outHMA, temp1, temp2, temp3);
        for (let i = 0; i < n; i += 1) data[i].hma35 = outHMA[i];
    }
    if (flags.hma55) {
        getHMAArray(closeArray, cfg.hma55.length, outHMA, temp1, temp2, temp3);
        for (let i = 0; i < n; i += 1) data[i].hma55 = outHMA[i];
    }
    if (flags.hma80) {
        getHMAArray(closeArray, 80, outHMA, temp1, temp2, temp3);
        for (let i = 0; i < n; i += 1) data[i].hma80 = outHMA[i];
    }
    if (flags.hma100) {
        getHMAArray(closeArray, cfg.hma100.length, outHMA, temp1, temp2, temp3);
        for (let i = 0; i < n; i += 1) data[i].hma100 = outHMA[i];
    }
    if (flags.devCloud) applyChartDeviationCloud(data, cfg.devCloud.emaLength, cfg.devCloud.basisMode);

    if (flags.dif) {
        const difCfg = cfg.dif || DEFAULT_CHART_SETTINGS.indicators.dif;
        const rsiLen = Math.max(1, Math.round(Number(difCfg.rsiLength) || 9));
        const hmLen = Math.max(1, Math.round(Number(difCfg.hmLength) || 60));
        const hmLen2 = Math.max(1, Math.round(Number(difCfg.hmLength2) || 36));
        const hmLen3 = Math.max(1, Math.round(Number(difCfg.hmLength3) || 22));
        const rsiSmaLowLen = Math.max(1, Math.round(Number(difCfg.rsiSmaLowLength) || 9));
        const rsiSmaLen = Math.max(1, Math.round(Number(difCfg.rsiSmaLength) || 31));
        const krsi = Number(difCfg.krsi) || 30;
        const kWidth = Number(difCfg.kWidth) || 1.0;
        const kWidth2 = Number(difCfg.kWidth2) || 0.5;
        const inverse = difCfg.inverse === true;

        const maxBase = inverse ? 100 : 0;
        const sign = inverse ? -1 : 1;
        const plotVal = (val) => Number.isFinite(val) ? maxBase + sign * val : NaN;

        const rsiArray = getReusableFloat64Buffer("dif_rsi", n);
        const hma50Array = getReusableFloat64Buffer("dif_hma50", n);
        const hmaLen2Array = getReusableFloat64Buffer("dif_hma_len2", n);
        const hmaLen3Array = getReusableFloat64Buffer("dif_hma_len3", n);
        const hma25Array = getReusableFloat64Buffer("dif_hma25_close", n);
        const rsiSmaLowArray = getReusableFloat64Buffer("dif_rsi_sma_low", n);
        const rsiSmaArray = getReusableFloat64Buffer("dif_rsi_sma", n);
        const volMa50Array = getReusableFloat64Buffer("dif_vol_ma50", n);
        const volMa100Array = getReusableFloat64Buffer("dif_vol_ma100", n);

        // 1. Calculate raw RSI of close
        rsiArray.fill(NaN);
        if (n > rsiLen) {
            let avgGain = 0;
            let avgLoss = 0;

            for (let i = 1; i <= rsiLen; i += 1) {
                const diff = closeArray[i] - closeArray[i - 1];
                if (diff > 0) {
                    avgGain += diff;
                } else {
                    avgLoss -= diff;
                }
            }
            avgGain /= rsiLen;
            avgLoss /= rsiLen;

            const firstSum = avgGain + avgLoss;
            rsiArray[rsiLen] = firstSum === 0 ? 100 : (100 * avgGain) / firstSum;

            const invRsiLen = 1 / rsiLen;
            const rsiLenMinus1 = rsiLen - 1;
            for (let i = rsiLen + 1; i < n; i += 1) {
                const diff = closeArray[i] - closeArray[i - 1];
                const gain = diff > 0 ? diff : 0;
                const loss = diff < 0 ? -diff : 0;

                avgGain = (avgGain * rsiLenMinus1 + gain) * invRsiLen;
                avgLoss = (avgLoss * rsiLenMinus1 + loss) * invRsiLen;

                const sum = avgGain + avgLoss;
                rsiArray[i] = sum === 0 ? 100 : (100 * avgGain) / sum;
            }
        }

        // 2. Calculate HMAs of raw_rsi, and HM25 of close
        getHMAArray(rsiArray, hmLen, hma50Array, temp1, temp2, temp3);
        getHMAArray(rsiArray, hmLen2, hmaLen2Array, temp1, temp2, temp3);
        getHMAArray(rsiArray, hmLen3, hmaLen3Array, temp1, temp2, temp3);
        getHMAArray(closeArray, 25, hma25Array, temp1, temp2, temp3);

        // 3. Calculate EMA (rsiSmaLowLength) and SMA (rsiSmaLength) of raw_rsi
        getEMAArray(rsiArray, rsiSmaLowLen, rsiSmaLowArray);
        getSMAArray(rsiArray, rsiSmaLen, rsiSmaArray);

        // 4. Calculate Volume Indicators
        getSMAArray(volumeArray, 50, volMa50Array);
        getSMAArray(volumeArray, 100, volMa100Array);

        const btcOutVol = getReusableFloat64Buffer("dif_btc_out_vol", n);
        const volPlotArray = getReusableFloat64Buffer("dif_vol_plot", n);

        for (let i = 0; i < n; i += 1) {
            const vol = volumeArray[i];
            const ma50 = volMa50Array[i];
            const ma100 = volMa100Array[i];

            if (Number.isNaN(ma50) || Number.isNaN(ma100) || ma50 === 0 || ma100 === 0) {
                btcOutVol[i] = 5;
                volPlotArray[i] = 0;
                continue;
            }

            let oscSv = 0.5 * (50 * (vol - ma50) / ma50);
            if (oscSv > 72) oscSv = 72;
            const oscSvBTC = oscSv / 2;
            const volBTC = (oscSvBTC - 5) / 2 + 10;

            let volSt = 0;
            if (vol > ma100 && vol < ma100 * 2) {
                volSt = 7;
            } else if (vol > ma100 * 2 && vol < ma100 * 4) {
                volSt = 15;
            } else if (vol > ma100 * 4 && vol < ma100 * 8) {
                volSt = 27;
            } else if (vol > ma100 * 8) {
                volSt = 37;
            }

            let volPlot = 0;
            if (volBTC > 5) {
                volPlot = volBTC > volSt ? volBTC : volSt;
            } else {
                volPlot = 0;
            }

            btcOutVol[i] = volPlot;
            volPlotArray[i] = volPlot;
        }

        // 5. Apply plotVal inversion, compile color codes and map to schema fields
        const k_kWidth2 = krsi * kWidth2;
        const k_kWidth = krsi * kWidth;

        for (let i = 0; i < n; i += 1) {
            const c = data[i];
            const rawRsi = rsiArray[i];
            const hm50 = hma50Array[i];
            const hmLen2 = hmaLen2Array[i];
            const hmLen3 = hmaLen3Array[i];
            const rsiSmaLow = rsiSmaLowArray[i];
            const rsiSma = rsiSmaArray[i];
            const hm25 = hma25Array[i];
            const volCur = btcOutVol[i];
            const volPlot = volPlotArray[i];

            // Save intermediate keys for compatibility
            c._raw_rsi = rawRsi;
            c._hm50 = hm50;
            c._hmLen2 = hmLen2;
            c._hmLen3 = hmLen3;
            c._hm25 = hm25;
            c._rsi_sma_low = rsiSmaLow;
            c._rsi_sma = rsiSma;
            c._vol_ma50 = volMa50Array[i];
            c._vol_ma100 = volMa100Array[i];
            c._BTC_out_vol = volCur;
            c._volPlot = volPlot;

            // Plot outputs
            c.dif_rsi = plotVal(rawRsi);
            c.dif_hmLen2 = plotVal(hmLen2);
            c.dif_hmLen3 = plotVal(hmLen3);
            c.dif_hm50 = plotVal(hm50);
            c.dif_rsi_sma_low = plotVal(rsiSmaLow);
            c.dif_rsi_sma = plotVal(rsiSma);

            // Envelopes around rsiSma
            if (Number.isFinite(rsiSma)) {
                c.dif_env_inner_up = plotVal(rsiSma + k_kWidth2);
                c.dif_env_inner_down = plotVal(rsiSma - k_kWidth2);
                c.dif_env_outer_up = plotVal(rsiSma + k_kWidth);
                c.dif_env_outer_down = plotVal(rsiSma - k_kWidth);
            } else {
                c.dif_env_inner_up = NaN;
                c.dif_env_inner_down = NaN;
                c.dif_env_outer_up = NaN;
                c.dif_env_outer_down = NaN;
            }

            c.dif_vol = volCur;
            c.dif_vol_plot = volPlot;

            // low < hm25?
            c.dif_low_under_hma = (Number.isFinite(hm25) && c.low < hm25) ? 1 : 0;

            // Fill color code logic
            let fillCode = 2; // Default (teal)

            if (Number.isFinite(rawRsi) && Number.isFinite(hm50) && Number.isFinite(hmLen2) && rawRsi < hm50 && rawRsi > hmLen2) {
                fillCode = 0; // Blue
            } else {
                const volPrev1 = (i > 0) ? btcOutVol[i - 1] : 0;
                const volPrev2 = (i > 1) ? btcOutVol[i - 2] : 0;

                const hasSpike = (volCur > 7.5 && volPrev1 > 7.5 && volPrev2 > 5) || (volCur > 15 && Number.isFinite(rsiSmaLow) && rawRsi < rsiSmaLow);
                if (hasSpike) {
                    fillCode = 1; // Red
                }
            }
            c.dif_fill_color_code = fillCode;
        }
    }

    if (state.settings.scalping?.enabled) {
        computeCvdAndAdvancedMetrics(data, token);
    }
    return data;
}

function isTradeOnlyTimeframe(timeframe) {
    if (timeframe === "1s") {
        return !state.indicatorsOn1sEnabled;
    }
    return false;
}

function isChartHistoryMode(chartId) {
    return state.chartHistoryModes.get(Number(chartId)) === true;
}

function setChartHistoryMode(chartId, enabled) {
    const normalizedChartId = Number(chartId);
    if (!Number.isFinite(normalizedChartId)) return false;
    const nextEnabled = enabled === true;
    state.chartHistoryModes.set(normalizedChartId, nextEnabled);
    const chart = state.charts.get(normalizedChartId);
    if (chart) chart.historyMode = nextEnabled;
    return nextEnabled;
}

function abortChartRequest(chartId, chartGeneration = null, reason = "cancelled") {
    const normalizedChartId = Number(chartId);
    if (!Number.isFinite(normalizedChartId)) return false;
    const current = state.chartRequestContexts.get(normalizedChartId);
    if (!current) return false;
    const expectedGeneration = chartGeneration === null || chartGeneration === undefined
        ? null
        : Math.max(0, Math.floor(Number(chartGeneration) || 0));
    if (expectedGeneration !== null && current.generation !== expectedGeneration) return false;
    state.chartRequestContexts.delete(normalizedChartId);
    if (!current.controller.signal.aborted) {
        try {
            current.controller.abort(new DOMException(String(reason || "cancelled"), "AbortError"));
        } catch {
            current.controller.abort();
        }
    }
    return true;
}

function abortAllChartRequests(reason = "cancelled") {
    for (const chartId of Array.from(state.chartRequestContexts.keys())) {
        abortChartRequest(chartId, null, reason);
    }
}

function beginChartRequest(payload) {
    const chartId = Number(payload?.chartId);
    abortChartRequest(chartId, null, "superseded");
    const controller = new AbortController();
    const context = {
        chartId,
        tokenKey: getTokenKey(payload?.token),
        timeframe: String(payload?.timeframe || ""),
        generation: Math.max(0, Math.floor(Number(payload?.chartGeneration) || 0)),
        serial: ++state.chartRequestSerial,
        controller,
        signal: controller.signal
    };
    state.chartRequestContexts.set(chartId, context);
    return context;
}

function isChartRequestCurrent(context) {
    if (!context) return false;
    const current = state.chartRequestContexts.get(context.chartId);
    return Boolean(
        current
        && current.serial === context.serial
        && current.generation === context.generation
        && current.tokenKey === context.tokenKey
        && current.timeframe === context.timeframe
    );
}

function createStaleChartResult(context, extra = {}) {
    return {
        chartId: context?.chartId,
        tokenKey: context?.tokenKey || "",
        timeframe: context?.timeframe || "",
        chartGeneration: context?.generation || 0,
        stale: true,
        candles: [],
        ...extra
    };
}

function isStoredChartCompatible(stored, token, timeframe) {
    return Boolean(
        stored
        && getTokenKey(stored.token) === getTokenKey(token)
        && stored.timeframe === timeframe
    );
}

async function fetchCandlesForChart(token, timeframe, limit, signal, endTime = null) {
    const market = getTokenMarket(token);
    if (market === MARKET.FUTURES && timeframe === "1s") {
        return fetchFuturesOneSecondCandles(token, limit, signal);
    }
    const normalizedEndTime = normalizeEpochMilliseconds(endTime);
    const payload = await fetchJsonWithTimeout(
        getKlinesUrl(token, timeframe, limit, null, normalizedEndTime > 0 ? normalizedEndTime : null),
        { signal },
        10000
    );
    return mapChartCandleData(market === MARKET.ALPHA ? (payload?.data || payload) : payload).slice(-CHART_MAX_CANDLES);
}

async function fetchCandlesForChartRange(token, timeframe, startTime, endTime, limit, signal) {
    if (timeframe === "1s") return [];
    const market = getTokenMarket(token);
    const safeLimit = Math.max(1, Math.min(CHART_MAX_CANDLES, Number(limit) || CHART_MAX_CANDLES));
    const payload = await fetchJsonWithTimeout(getKlinesUrl(token, timeframe, safeLimit, startTime, endTime), { signal }, 15000);
    const mapped = mapChartCandleData(market === MARKET.ALPHA ? (payload?.data || payload) : payload);
    const startSeconds = Math.floor(normalizeEpochMilliseconds(startTime) / 1000);
    const endSeconds = Math.ceil(normalizeEpochMilliseconds(endTime) / 1000);
    const filtered = mapped.filter(candle => {
        const time = Number(candle?.time);
        return Number.isFinite(time)
            && (startSeconds <= 0 || time >= startSeconds)
            && (endSeconds <= 0 || time <= endSeconds);
    });
    return filtered.slice(0, safeLimit);
}

function normalizeRawCandles(rawCandles, token = state.selectedToken) {
    const data = (rawCandles || []).map(({ time, open, high, low, close, volume, takerBuyVolume }) => ({
        time, open, high, low, close,
        volume: Number.isFinite(volume) ? volume : 0,
        takerBuyVolume: Number.isFinite(takerBuyVolume) ? takerBuyVolume : 0
    }));
    if (state.settings.scalping?.enabled) {
        computeCvdAndAdvancedMetrics(data, token);
    }
    return data;
}

async function loadChartInitial(payload) {
    setChartHistoryMode(payload?.chartId, false);
    const opStarted = perfNow();
    const requestContext = beginChartRequest(payload);
    const { chartId, token, timeframe, settings, limit } = payload;
    const chartSettings = settings?.charts || settings || state.settings.charts || DEFAULT_CHART_SETTINGS;
    const runtimeState = resolveChartIndicatorRuntimeState(chartSettings, timeframe);
    const liveEndTime = payload?.forceLatest === true
        ? (normalizeEpochMilliseconds(payload?.liveEndTime) || nowMs())
        : 0;
    const requestedLimit = Math.max(1, Math.min(CHART_MAX_CANDLES, Number(limit) || CHART_MAX_CANDLES));
    const fetchStarted = perfNow();
    const [listingResolution, candles] = await Promise.all([
        resolveTokenListingTimeRecord(token, requestContext.signal),
        fetchCandlesForChart(
            token,
            timeframe,
            requestedLimit,
            requestContext.signal,
            liveEndTime
        )
    ]);
    if (!isChartRequestCurrent(requestContext)) {
        return createStaleChartResult(requestContext, { reason: "superseded_initial_fetch" });
    }
    recordWorkerPerf("chart_fetch_initial", fetchStarted, {
        chartId,
        timeframe,
        candles: candles.length,
        requestedLimit
    });
    const currentOpenSecond = timeframe === "1s" ? Math.floor(nowMs() / 1000) : 0;
    const normalizedSourceCandles = timeframe === "1s"
        ? fillOneSecondCandleGaps(candles, { currentOpenTime: currentOpenSecond })
        : candles;
    const calcStarted = perfNow();
    const enriched = isTradeOnlyTimeframe(timeframe)
        ? normalizeRawCandles(normalizedSourceCandles, token)
        : calculateChartIndicators(normalizedSourceCandles, chartSettings, timeframe, token);
    const includesDif = runtimeState.difActive === true && !isTradeOnlyTimeframe(timeframe);
    const transferFields = buildChartTransferFields(runtimeState, includesDif);
    recordWorkerPerf("chart_calc_initial", calcStarted, { chartId, timeframe, candles: enriched.length, includesDif, lightweight1s: runtimeState.lightweight1s });
    if (!isChartRequestCurrent(requestContext)) {
        return createStaleChartResult(requestContext, { reason: "superseded_initial_load" });
    }
    state.charts.set(chartId, { token, timeframe, candles: enriched, rawCandles: normalizeRawCandles(normalizedSourceCandles, token), updatedAt: nowMs() });
    state.workerDiagnostics.chartBuffers[chartId] = {
        symbol: token?.symbol || "—",
        timeframe,
        candles: enriched.length,
        lastCandleTime: enriched[enriched.length - 1]?.time || null,
        loading: false,
        includesDif
    };
    const currentOpenBucketMs = getTimeframeBucketStartMs(nowMs(), timeframe);
    const lastCandleTime = enriched[enriched.length - 1]?.time || 0;
    const containsCurrentOpenCandle = currentOpenBucketMs > 0
        && lastCandleTime >= Math.floor(currentOpenBucketMs / 1000);
    const listingBoundaryReached = hasReachedTokenListingBoundary(enriched, token, timeframe);
    // A short initial page remains a defensive fallback only when no explicit boundary
    // could be resolved from exchange metadata or the first available 1m candle.
    const initialPageReachedDataStart = listingResolution.listingTime <= 0
        && hasInitialKlinePageReachedDataStart(candles, requestedLimit, token, timeframe);
    const reachedHistoryStart = listingBoundaryReached || initialPageReachedDataStart;
    const historyStartReason = listingBoundaryReached
        ? String(listingResolution.source || "listing_boundary")
        : (initialPageReachedDataStart ? "initial_page_exhausted_fallback" : "");
    recordWorkerPerf("chart_load_initial", opStarted, {
        chartId,
        timeframe,
        candles: enriched.length,
        requestedLimit,
        includesDif,
        forceLatest: payload?.forceLatest === true,
        containsCurrentOpenCandle,
        listingBoundaryReached,
        initialPageReachedDataStart,
        reachedHistoryStart,
        historyStartReason,
        resolvedListingTime: listingResolution.listingTime,
        resolvedListingTimeSource: listingResolution.source
    });
    return {
        chartId,
        tokenKey: getTokenKey(token),
        timeframe,
        chartGeneration: requestContext.generation,
        candles: enriched,
        includesDif,
        runtimeState,
        transferFields,
        forceLatest: payload?.forceLatest === true,
        requestedLiveEndTime: liveEndTime,
        containsCurrentOpenCandle,
        reachedHistoryStart,
        listingBoundaryReached,
        initialPageReachedDataStart,
        historyStartReason,
        resolvedListingTime: listingResolution.listingTime,
        resolvedListingTimeSource: listingResolution.source,
        requestedLimit,
        lastCandleTime,
        initialLastAggregateTradeId: timeframe === "1s" && getTokenMarket(token) === MARKET.FUTURES
            ? (Number(futuresOneSecondInitialLastTradeIdBySymbol.get(getRequestSymbol(token))) || 0)
            : 0
    };
}

async function refreshChartTail(payload) {
    const { chartId, token, timeframe, settings, rawCandles, reason } = payload;
    if (isChartHistoryMode(chartId)) {
        return { chartId, tokenKey: getTokenKey(token), timeframe, chartGeneration: Math.max(0, Math.floor(Number(payload?.chartGeneration) || 0)), skipped: true, reason: "history_mode" };
    }
    const requestContext = beginChartRequest(payload);
    const refreshMode = String(payload?.refreshMode || "standard");
    const isOneSecond = timeframe === "1s";
    const isReconcile = isOneSecond && refreshMode === "reconcile";
    const isLocalOneSecond = isOneSecond && refreshMode === "local";
    const tradeOnly = isTradeOnlyTimeframe(timeframe);
    if (tradeOnly && !isReconcile) {
        return { chartId, tokenKey: getTokenKey(token), timeframe, chartGeneration: requestContext.generation, skipped: true, reason: "trade_only", refreshMode };
    }

    const chartSettings = settings?.charts || settings || state.settings.charts || DEFAULT_CHART_SETTINGS;
    const runtimeState = resolveChartIndicatorRuntimeState(chartSettings, timeframe);
    const isFuturesOneSecond = getTokenMarket(token) === MARKET.FUTURES && isOneSecond;
    if (isReconcile && isFuturesOneSecond) {
        return { chartId, tokenKey: getTokenKey(token), timeframe, chartGeneration: requestContext.generation, skipped: true, reason: "futures_1s_no_kline", refreshMode };
    }

    const protectedOpenTime = Math.max(0, Math.floor(Number(payload?.protectedOpenTime) || 0));
    let tail = [];
    if (isReconcile) {
        tail = await fetchOneSecondReconcileTail(token, payload?.tailLimit, requestContext.signal);
    } else if (!isLocalOneSecond && !isFuturesOneSecond) {
        tail = await fetchCandlesForChart(token, timeframe, CHART_TAIL_REFRESH_LIMIT, requestContext.signal);
    }
    if (!isChartRequestCurrent(requestContext)) {
        return createStaleChartResult(requestContext, { reason: "superseded_tail_fetch", refreshMode });
    }

    const storedChart = state.charts.get(chartId);
    const storedChartCompatible = isStoredChartCompatible(storedChart, token, timeframe);
    const rawBase = normalizeRawCandles(rawCandles || (storedChartCompatible ? storedChart.rawCandles : []) || [], token).slice(-CHART_MAX_CANDLES);
    let mergedRaw = rawBase;
    let reconcileReplaceStartIndex = rawBase.length;
    let correctedCount = 0;
    if (isReconcile) {
        const closedCandleLimit = Math.max(1, Math.min(100, (Number(payload?.tailLimit) || ONE_SECOND_RECONCILE_FETCH_LIMIT) - 2));
        const closedTail = tail
            .filter(candle => protectedOpenTime <= 0 || Number(candle?.time) < protectedOpenTime)
            .slice(-closedCandleLimit);
        const reconciliation = mergeOneSecondReconcileCandles(rawBase, closedTail, protectedOpenTime);
        mergedRaw = reconciliation.merged;
        reconcileReplaceStartIndex = reconciliation.replaceStartIndex;
        correctedCount = reconciliation.correctedCount;
        const requestedDirtyFromTime = Math.max(0, Math.floor(Number(payload.replaceFromTime) || 0));
        if (correctedCount === 0 && requestedDirtyFromTime <= 0) {
            return {
                chartId,
                tokenKey: getTokenKey(token),
                timeframe,
                chartGeneration: requestContext.generation,
                skipped: true,
                unchanged: true,
                reason,
                refreshMode,
                protectedOpenTime,
                correctedCount: 0
            };
        }
    } else if (!isLocalOneSecond && !isFuturesOneSecond) {
        mergedRaw = mergeChartCandles(rawBase, tail);
    }

    if (!mergedRaw.length) {
        return {
            chartId,
            tokenKey: getTokenKey(token),
            timeframe,
            chartGeneration: requestContext.generation,
            ...(isOneSecond ? { indicatorOnly: true, indicatorCandles: [], rawCorrections: [] } : { candles: [] }),
            replaceStartIndex: 0,
            fullReset: true,
            reason,
            refreshMode,
            protectedOpenTime,
            correctedCount
        };
    }

    const previous = storedChartCompatible ? (storedChart.candles || []) : [];
    const trimmedMergedRaw = mergedRaw.slice(-CHART_MAX_CANDLES);
    const previousFirstTime = previous[0]?.time;
    const nextFirstTime = trimmedMergedRaw[0]?.time;
    const structuralResetNeeded = !storedChartCompatible
        || previous.length > trimmedMergedRaw.length
        || previousFirstTime !== nextFirstTime
        || payload.needsIndicatorFullResync === true;
    const calculate = source => tradeOnly
        ? normalizeRawCandles(source, token)
        : calculateChartIndicators(source, chartSettings, timeframe, token);
    const includesDif = tradeOnly ? false : runtimeState.difActive === true;
    const transferFields = buildChartTransferFields(runtimeState, includesDif);
    const indicatorTransferFields = buildIndicatorTransferFields(runtimeState, includesDif);

    if (structuralResetNeeded) {
        const calcStarted = perfNow();
        const enriched = calculate(mergedRaw);
        recordWorkerPerf("chart_calc_tail_full", calcStarted, { chartId, timeframe, candles: enriched.length, includesDif, lightweight1s: runtimeState.lightweight1s, refreshMode, correctedCount });
        if (!isChartRequestCurrent(requestContext)) {
            return createStaleChartResult(requestContext, { reason: "superseded_tail_calculation", refreshMode });
        }
        state.charts.set(chartId, { token, timeframe, candles: enriched, rawCandles: normalizeRawCandles(mergedRaw, token), updatedAt: nowMs() });
        state.workerDiagnostics.chartBuffers[chartId] = {
            symbol: token?.symbol || "—",
            timeframe,
            candles: enriched.length,
            lastCandleTime: enriched[enriched.length - 1]?.time || null,
            loading: false,
            includesDif
        };
        if (isOneSecond) {
            const rawCorrections = isReconcile
                ? mergedRaw.filter(candle => Number(candle?.time) < protectedOpenTime)
                : [];
            return {
                chartId,
                tokenKey: getTokenKey(token),
                timeframe,
                chartGeneration: requestContext.generation,
                indicatorOnly: true,
                indicatorCandles: enriched.filter(candle => protectedOpenTime <= 0 || Number(candle?.time) < protectedOpenTime),
                rawCorrections,
                replaceStartIndex: 0,
                fullReset: true,
                reason,
                refreshMode,
                protectedOpenTime,
                correctedCount,
                includesDif,
                runtimeState,
                indicatorTransferFields
            };
        }
        return { chartId, tokenKey: getTokenKey(token), timeframe, chartGeneration: requestContext.generation, candles: enriched, replaceStartIndex: 0, fullReset: true, reason, refreshMode, protectedOpenTime, correctedCount, includesDif, runtimeState, transferFields };
    }

    let replaceStartIndex = mergedRaw.length;
    const replaceFromTime = Math.max(0, Math.floor(Number(payload.replaceFromTime) || 0));
    if (isReconcile) {
        replaceStartIndex = reconcileReplaceStartIndex;
        if (replaceFromTime > 0) {
            const requestedIndex = mergedRaw.findIndex(candle => candle.time >= replaceFromTime);
            if (requestedIndex >= 0) replaceStartIndex = Math.min(replaceStartIndex, requestedIndex);
        }
        if (replaceStartIndex === mergedRaw.length) replaceStartIndex = Math.max(0, mergedRaw.length - 2);
    } else if (isLocalOneSecond || isFuturesOneSecond) {
        if (replaceFromTime > 0) {
            const requestedIndex = mergedRaw.findIndex(candle => candle.time >= replaceFromTime);
            if (requestedIndex >= 0) replaceStartIndex = requestedIndex;
        }
        if (replaceStartIndex === mergedRaw.length) replaceStartIndex = Math.max(0, mergedRaw.length - 2);
    } else {
        const timeToIndex = new Map();
        for (let index = 0; index < mergedRaw.length; index += 1) timeToIndex.set(mergedRaw[index].time, index);
        for (const candle of tail) {
            const index = timeToIndex.get(candle.time);
            if (index !== undefined) replaceStartIndex = Math.min(replaceStartIndex, index);
        }
        if (replaceStartIndex === mergedRaw.length) replaceStartIndex = Math.max(0, mergedRaw.length - tail.length);
    }

    const warmupBars = Math.max(
        getChartIndicatorWarmupBars(chartSettings, timeframe),
        getScalpingIndicatorWarmupBars()
    );
    const calcStartIndex = Math.max(0, replaceStartIndex - warmupBars);
    const calcWindow = mergedRaw.slice(calcStartIndex);
    const calcStarted = perfNow();
    const recalculatedWindow = calculate(calcWindow);
    recordWorkerPerf("chart_calc_tail_incremental", calcStarted, { chartId, timeframe, candles: calcWindow.length, replaceStartIndex, includesDif, lightweight1s: runtimeState.lightweight1s, refreshMode, correctedCount });
    const replacementOffset = replaceStartIndex - calcStartIndex;
    const replacementTail = recalculatedWindow.slice(replacementOffset);
    const previousPrefix = previous.slice(0, replaceStartIndex);
    const combined = previousPrefix.concat(replacementTail).slice(-CHART_MAX_CANDLES);

    if (!isChartRequestCurrent(requestContext)) {
        return createStaleChartResult(requestContext, { reason: "superseded_tail_merge", refreshMode });
    }
    state.charts.set(chartId, { token, timeframe, candles: combined, rawCandles: normalizeRawCandles(mergedRaw, token), updatedAt: nowMs() });
    state.workerDiagnostics.chartBuffers[chartId] = {
        symbol: token?.symbol || "—",
        timeframe,
        candles: combined.length,
        lastCandleTime: combined[combined.length - 1]?.time || null,
        loading: false,
        includesDif
    };
    if (isOneSecond) {
        const rawCorrections = isReconcile
            ? mergedRaw.slice(replaceStartIndex).filter(candle => Number(candle?.time) < protectedOpenTime)
            : [];
        return {
            chartId,
            tokenKey: getTokenKey(token),
            timeframe,
            chartGeneration: requestContext.generation,
            indicatorOnly: true,
            indicatorCandles: combined
                .slice(replaceStartIndex)
                .filter(candle => protectedOpenTime <= 0 || Number(candle?.time) < protectedOpenTime),
            rawCorrections,
            replaceStartIndex,
            fullReset: false,
            reason,
            refreshMode,
            protectedOpenTime,
            correctedCount,
            includesDif,
            runtimeState,
            indicatorTransferFields
        };
    }
    return { chartId, tokenKey: getTokenKey(token), timeframe, chartGeneration: requestContext.generation, candles: combined, replaceStartIndex, fullReset: false, reason, refreshMode, protectedOpenTime, correctedCount, includesDif, runtimeState, transferFields };
}

async function loadChartHistoryWindow(payload) {
    const { chartId, token, timeframe, settings, startTime, endTime, limit, direction } = payload;
    if (timeframe === "1s") {
        return {
            chartId,
            tokenKey: getTokenKey(token),
            timeframe,
            chartGeneration: Math.max(0, Math.floor(Number(payload?.chartGeneration) || 0)),
            candles: [],
            bigBuySignals: [],
            bigSellSignals: [],
            skipped: true,
            reason: "history_not_supported_for_1s",
            reachedHistoryStart: false,
            containsCurrentOpenCandle: false
        };
    }

    setChartHistoryMode(chartId, true);
    const requestContext = beginChartRequest(payload);
    const chartSettings = settings?.charts || settings || state.settings.charts || DEFAULT_CHART_SETTINGS;
    const runtimeState = resolveChartIndicatorRuntimeState(chartSettings, timeframe);
    const requestedStart = normalizeEpochMilliseconds(startTime);
    const requestedEnd = normalizeEpochMilliseconds(endTime);
    const listingResolution = await resolveTokenListingTimeRecord(token, requestContext.signal);
    if (!isChartRequestCurrent(requestContext) || !isChartHistoryMode(chartId)) {
        return createStaleChartResult(requestContext, { reason: "superseded_history_listing_time_resolution" });
    }
    const listingTime = listingResolution.listingTime;
    const listingBucket = listingTime > 0 ? (getTimeframeBucketStartMs(listingTime, timeframe) || listingTime) : 0;
    const effectiveStart = Math.max(requestedStart, listingBucket || 0);
    const intervalMs = getEstimatedTimeframeIntervalMs(timeframe);
    const safeLimit = Math.max(1, Math.min(CHART_MAX_CANDLES, Number(limit) || CHART_MAX_CANDLES));

    if (requestedEnd <= 0 || effectiveStart >= requestedEnd) {
        return {
            chartId,
            tokenKey: getTokenKey(token),
            timeframe,
            chartGeneration: requestContext.generation,
            candles: [],
            bigBuySignals: [],
            bigSellSignals: [],
            reachedHistoryStart: listingBucket > 0,
            containsCurrentOpenCandle: false,
            actualStartTime: 0,
            actualEndTime: 0,
            direction,
            resolvedListingTime: listingResolution.listingTime,
            resolvedListingTimeSource: listingResolution.source
        };
    }

    const fetchStarted = perfNow();
    const rawCandles = await fetchCandlesForChartRange(token, timeframe, effectiveStart, requestedEnd, safeLimit, requestContext.signal);
    recordWorkerPerf("chart_fetch_history", fetchStarted, { chartId, timeframe, candles: rawCandles.length, direction });
    if (!isChartRequestCurrent(requestContext) || !isChartHistoryMode(chartId)) {
        return createStaleChartResult(requestContext, { reason: "superseded_history_fetch" });
    }

    const calcStarted = perfNow();
    const enriched = calculateChartIndicators(rawCandles, chartSettings, timeframe, token);
    const includesDif = runtimeState.difActive === true;
    const transferFields = buildChartTransferFields(runtimeState, includesDif);
    recordWorkerPerf("chart_calc_history", calcStarted, { chartId, timeframe, candles: enriched.length, includesDif });
    if (!isChartRequestCurrent(requestContext) || !isChartHistoryMode(chartId)) {
        return createStaleChartResult(requestContext, { reason: "superseded_history_calculation" });
    }

    const firstCandleTime = enriched[0]?.time || 0;
    const lastCandleTime = enriched[enriched.length - 1]?.time || 0;
    const firstCandleMs = firstCandleTime * 1000;
    const currentOpenBucketMs = getTimeframeBucketStartMs(nowMs(), timeframe);
    const normalizedDirection = direction === "newer" ? "newer" : "older";
    const reachedByListing = listingBucket > 0
        && (effectiveStart <= listingBucket + intervalMs || (firstCandleMs > 0 && firstCandleMs <= listingBucket + intervalMs));
    // Missing candles near the requested start can prove the left boundary only while
    // loading older data. Applying this heuristic to a newer request corrupts the
    // left-scroll state after a no-progress or sparse right-side response.
    const reachedByDataStart = normalizedDirection === "older" && (
        enriched.length === 0
        || (firstCandleMs > 0 && firstCandleMs > effectiveStart + intervalMs * 2)
    );
    const reachedHistoryStart = reachedByListing || reachedByDataStart;
    const containsCurrentOpenCandle = currentOpenBucketMs > 0 && lastCandleTime >= Math.floor(currentOpenBucketMs / 1000);

    // 1m history keeps its original signal load coupled to the 1m chart request.
    // 5m significant-buy markers are intentionally NOT loaded here: V165 uses one
    // dedicated bulk IndexedDB range read for the exact 5m candle interval, keeping
    // its depth and lifecycle fully independent from the 1m runtime/cache.
    let bigBuySignals = [];
    let bigSellSignals = [];
    const shouldLoadOneMinuteSignals = timeframe === "1m";
    if (shouldLoadOneMinuteSignals
        && firstCandleTime > 0
        && lastCandleTime >= firstCandleTime) {
        const identity = getBigBuySignalDatabaseIdentity(token);
        [bigBuySignals, bigSellSignals] = await Promise.all([
            readBigBuyMinuteSignalsFromDatabaseRange(identity, firstCandleTime, lastCandleTime),
            readBigSellMinuteSignalsFromDatabaseRange(identity, firstCandleTime, lastCandleTime)
        ]);
        if (!isChartRequestCurrent(requestContext) || !isChartHistoryMode(chartId)) {
            return createStaleChartResult(requestContext, { reason: "superseded_history_signal_load" });
        }
    }

    state.charts.set(chartId, {
        token,
        timeframe,
        candles: enriched,
        rawCandles: normalizeRawCandles(rawCandles, token),
        updatedAt: nowMs(),
        historyMode: true
    });
    state.workerDiagnostics.chartBuffers[chartId] = {
        symbol: token?.symbol || "—",
        timeframe,
        candles: enriched.length,
        lastCandleTime: lastCandleTime || null,
        loading: false,
        includesDif,
        historyMode: true
    };

    return {
        chartId,
        tokenKey: getTokenKey(token),
        timeframe,
        chartGeneration: requestContext.generation,
        candles: enriched,
        bigBuySignals,
        bigSellSignals,
        reachedHistoryStart,
        containsCurrentOpenCandle,
        actualStartTime: firstCandleMs,
        actualEndTime: lastCandleTime * 1000,
        direction,
        resolvedListingTime: listingResolution.listingTime,
        resolvedListingTimeSource: listingResolution.source,
        includesDif,
        runtimeState,
        transferFields
    };
}

function recalculateChart(payload) {
    const opStarted = perfNow();
    const requestContext = beginChartRequest(payload);
    const { chartId, token, timeframe, settings, rawCandles } = payload;
    const chartSettings = settings?.charts || settings || state.settings.charts || DEFAULT_CHART_SETTINGS;
    const runtimeState = resolveChartIndicatorRuntimeState(chartSettings, timeframe);
    const raw = normalizeRawCandles(rawCandles || state.charts.get(chartId)?.rawCandles || [], token);
    const enriched = isTradeOnlyTimeframe(timeframe) ? raw : calculateChartIndicators(raw, chartSettings, timeframe, token);
    const includesDif = runtimeState.difActive === true && !isTradeOnlyTimeframe(timeframe);
    const indicatorTransferFields = buildIndicatorTransferFields(runtimeState, includesDif);
    if (!isChartRequestCurrent(requestContext)) {
        return createStaleChartResult(requestContext, { reason: "superseded_recalculation" });
    }
    state.charts.set(chartId, { token, timeframe, candles: enriched, rawCandles: raw, updatedAt: nowMs() });
    state.workerDiagnostics.chartBuffers[chartId] = {
        symbol: token?.symbol || "—",
        timeframe,
        candles: enriched.length,
        lastCandleTime: enriched[enriched.length - 1]?.time || null,
        loading: false,
        includesDif
    };
    recordWorkerPerf("chart_recalculate", opStarted, { chartId, timeframe, candles: enriched.length, includesDif });
    return {
        chartId,
        tokenKey: getTokenKey(token),
        timeframe,
        chartGeneration: requestContext.generation,
        indicatorOnly: true,
        indicatorCandles: enriched,
        includesDif,
        runtimeState,
        indicatorTransferFields
    };
}


const ADVANCED_TRADE_THRESHOLD_METHOD = "hybrid_big_plus_tail_outliers_v1";
const ADVANCED_TRADE_THRESHOLD_DEFAULTS = Object.freeze({
    bigTradesThresholdUsd: 5000,
    significantTradesThresholdUsd: 9000
});

function createLogNotionalReservoir(capacity) {
    const safeCapacity = Math.max(1, Math.floor(Number(capacity) || 1));
    return {
        values: new Float64Array(safeCapacity),
        length: 0,
        seen: 0
    };
}

function resetLogNotionalReservoir(reservoir) {
    if (!reservoir) return;
    reservoir.length = 0;
    reservoir.seen = 0;
}

function addLogNotionalToReservoir(reservoir, logNotional) {
    if (!reservoir || !Number.isFinite(logNotional)) return;
    reservoir.seen += 1;
    if (reservoir.length < reservoir.values.length) {
        reservoir.values[reservoir.length] = logNotional;
        reservoir.length += 1;
        return;
    }
    const replacementIndex = Math.floor(Math.random() * reservoir.seen);
    if (replacementIndex < reservoir.values.length) {
        reservoir.values[replacementIndex] = logNotional;
    }
}

function createTimedRelativeStrengthBuffer(capacity) {
    const safeCapacity = Math.max(16, Math.floor(Number(capacity) || 16));
    return {
        logRatios: new Float64Array(safeCapacity),
        timestamps: new Float64Array(safeCapacity),
        length: 0,
        writeIndex: 0,
        seen: 0
    };
}

function resetTimedRelativeStrengthBuffer(buffer) {
    if (!buffer) return;
    buffer.length = 0;
    buffer.writeIndex = 0;
    buffer.seen = 0;
}

function addTimedRelativeStrengthSample(buffer, logRatio, timestamp) {
    if (!buffer || !Number.isFinite(logRatio) || logRatio < 0) return false;
    const time = Math.max(0, Number(timestamp) || nowMs());
    const capacity = buffer.logRatios.length;
    const index = buffer.length < capacity ? buffer.length : buffer.writeIndex;
    buffer.logRatios[index] = logRatio;
    buffer.timestamps[index] = time;
    buffer.seen += 1;
    if (buffer.length < capacity) {
        buffer.length += 1;
        buffer.writeIndex = buffer.length % capacity;
    } else {
        buffer.writeIndex = (buffer.writeIndex + 1) % capacity;
    }
    return true;
}

function getRecentRelativeStrengthSamples(buffer, maximumAgeMs, evaluatedAt = nowMs()) {
    const length = Math.max(0, Math.floor(Number(buffer?.length) || 0));
    if (!buffer || length === 0) return new Float64Array(0);
    const cutoff = Math.max(0, evaluatedAt - Math.max(1, Number(maximumAgeMs) || 1));
    const values = new Float64Array(length);
    let used = 0;
    for (let index = 0; index < length; index += 1) {
        const timestamp = buffer.timestamps[index];
        const logRatio = buffer.logRatios[index];
        if (timestamp < cutoff || timestamp > evaluatedAt + 60000 || !Number.isFinite(logRatio) || logRatio < 0) continue;
        values[used] = logRatio;
        used += 1;
    }
    return values.slice(0, used);
}

function quantileFromSortedFloat64(sortedValues, quantile) {
    const length = sortedValues?.length || 0;
    if (length === 0) return NaN;
    if (length === 1) return sortedValues[0];
    const q = Math.min(1, Math.max(0, Number(quantile) || 0));
    const position = (length - 1) * q;
    const lowerIndex = Math.floor(position);
    const upperIndex = Math.ceil(position);
    if (lowerIndex === upperIndex) return sortedValues[lowerIndex];
    const weight = position - lowerIndex;
    return sortedValues[lowerIndex] + (sortedValues[upperIndex] - sortedValues[lowerIndex]) * weight;
}

function calculateUsdWeightedLogQuantile(sortedLogs, quantile, maximumWeightLog) {
    const length = sortedLogs?.length || 0;
    if (length === 0) return NaN;
    if (length === 1) return sortedLogs[0];

    const normalizedQuantile = Math.min(1, Math.max(0, Number(quantile) || 0));
    const largestObservedLog = sortedLogs[length - 1];
    const weightCapLog = Number.isFinite(maximumWeightLog)
        ? Math.min(largestObservedLog, maximumWeightLog)
        : largestObservedLog;
    let totalWeight = 0;
    for (let index = 0; index < length; index += 1) {
        const cappedLog = Math.min(sortedLogs[index], weightCapLog);
        totalWeight += Math.exp(cappedLog - weightCapLog);
    }
    if (!Number.isFinite(totalWeight) || totalWeight <= 0) return NaN;

    const targetWeight = totalWeight * normalizedQuantile;
    let cumulativeWeight = 0;
    for (let index = 0; index < length; index += 1) {
        const cappedLog = Math.min(sortedLogs[index], weightCapLog);
        cumulativeWeight += Math.exp(cappedLog - weightCapLog);
        if (cumulativeWeight >= targetWeight) return sortedLogs[index];
    }
    return sortedLogs[length - 1];
}

function calculateSegmentStatistics(prefixSum, prefixSquareSum, startIndex, endIndex) {
    const count = endIndex - startIndex;
    if (count <= 0) return null;
    const sum = prefixSum[endIndex] - prefixSum[startIndex];
    const squareSum = prefixSquareSum[endIndex] - prefixSquareSum[startIndex];
    const mean = sum / count;
    const sumSquaredError = Math.max(0, squareSum - (sum * sum) / count);
    return { count, mean, sumSquaredError };
}

function calculateUpperTailClusterBoundary(sortedLogs, tailPercentile, minimumClusterSamples) {
    const length = sortedLogs?.length || 0;
    const absoluteMinimum = Math.max(2, Math.floor(Number(minimumClusterSamples) || 2));
    const proportionalMinimum = Math.ceil(length * CONFIG.advancedTradeThresholdMinClusterFraction);
    const requiredClusterSamples = Math.max(absoluteMinimum, proportionalMinimum);
    if (length < requiredClusterSamples * 2) {
        return { valid: false, reason: "insufficient_cluster_samples" };
    }

    const normalizedTailPercentile = Math.min(0.999, Math.max(0.50, Number(tailPercentile) || 0.90));
    const requestedStartIndex = Math.floor((length - 1) * normalizedTailPercentile);
    const maximumStartIndex = length - requiredClusterSamples * 2;
    const tailStartIndex = Math.min(requestedStartIndex, maximumStartIndex);
    const tailLength = length - tailStartIndex;
    if (tailLength < requiredClusterSamples * 2) {
        return { valid: false, reason: "tail_too_small" };
    }

    // Centering keeps prefix-sum variance calculations stable when log values are close.
    const tailOriginLog = sortedLogs[tailStartIndex];
    const prefixSum = new Float64Array(tailLength + 1);
    const prefixSquareSum = new Float64Array(tailLength + 1);
    for (let localIndex = 0; localIndex < tailLength; localIndex += 1) {
        const centeredValue = sortedLogs[tailStartIndex + localIndex] - tailOriginLog;
        prefixSum[localIndex + 1] = prefixSum[localIndex] + centeredValue;
        prefixSquareSum[localIndex + 1] = prefixSquareSum[localIndex] + centeredValue * centeredValue;
    }

    const wholeTail = calculateSegmentStatistics(prefixSum, prefixSquareSum, 0, tailLength);
    if (!wholeTail || wholeTail.sumSquaredError <= Number.EPSILON) {
        return { valid: false, reason: "flat_tail" };
    }

    let best = null;
    const lastSplit = tailLength - requiredClusterSamples;
    for (let split = requiredClusterSamples; split <= lastSplit; split += 1) {
        const lower = calculateSegmentStatistics(prefixSum, prefixSquareSum, 0, split);
        const upper = calculateSegmentStatistics(prefixSum, prefixSquareSum, split, tailLength);
        if (!lower || !upper || upper.mean <= lower.mean) continue;
        const combinedError = lower.sumSquaredError + upper.sumSquaredError;
        if (!best || combinedError < best.combinedError) {
            best = { split, lower, upper, combinedError };
        }
    }
    if (!best) return { valid: false, reason: "no_cluster_split" };

    const varianceReduction = 1 - best.combinedError / wholeTail.sumSquaredError;
    const pooledDegreesOfFreedom = Math.max(1, tailLength - 2);
    const pooledStandardDeviation = Math.sqrt(best.combinedError / pooledDegreesOfFreedom);
    const separation = (best.upper.mean - best.lower.mean) / Math.max(0.05, pooledStandardDeviation);
    const upperStartIndex = tailStartIndex + best.split;
    const boundaryLog = sortedLogs[upperStartIndex];
    const candidateUsd = Math.exp(boundaryLog);
    const valid = Number.isFinite(candidateUsd)
        && candidateUsd > 0
        && varianceReduction >= CONFIG.advancedTradeThresholdClusterMinVarianceReduction
        && separation >= CONFIG.advancedTradeThresholdClusterMinSeparation;

    return {
        valid,
        reason: valid ? "ok" : "weak_cluster",
        tailStartIndex,
        tailLength,
        upperStartIndex,
        upperClusterCount: best.upper.count,
        lowerClusterCount: best.lower.count,
        varianceReduction,
        separation,
        boundaryLog,
        candidateUsd
    };
}

function calculateHybridLogThreshold(reservoir, options) {
    const params = options && typeof options === "object" ? options : {};
    const totalSeen = Math.max(0, Math.floor(Number(reservoir?.seen) || 0));
    const sampleLength = Math.max(0, Math.floor(Number(reservoir?.length) || 0));
    const robustMinimum = Math.max(1, Math.floor(Number(params.minimumSamples) || 1));
    const percentileMinimum = Math.max(robustMinimum, Math.floor(Number(params.minimumPercentileSamples) || robustMinimum));
    if (!reservoir || totalSeen < robustMinimum || sampleLength < Math.min(robustMinimum, reservoir.values.length)) {
        return {
            valid: false,
            totalSeen,
            sampleLength,
            estimator: "none",
            reason: "insufficient_samples"
        };
    }

    const sortedLogs = reservoir.values.slice(0, sampleLength);
    sortedLogs.sort();
    const medianLog = quantileFromSortedFloat64(sortedLogs, 0.5);
    const hasPercentileSupport = totalSeen >= percentileMinimum
        && sampleLength >= Math.min(percentileMinimum, reservoir.values.length);
    const percentileLog = hasPercentileSupport
        ? quantileFromSortedFloat64(sortedLogs, params.percentile)
        : NaN;

    const deviations = new Float64Array(sampleLength);
    for (let index = 0; index < sampleLength; index += 1) {
        deviations[index] = Math.abs(sortedLogs[index] - medianLog);
    }
    deviations.sort();
    const madLog = quantileFromSortedFloat64(deviations, 0.5);
    const robustSigmaLog = madLog * 1.4826;
    const madThresholdLog = medianLog + Math.max(0, Number(params.madMultiplier) || 0) * robustSigmaLog;

    // Cap per-trade USD weight relative to a robust baseline so one erroneous or exceptional print cannot dominate.
    const robustWeightBaseLog = Number.isFinite(percentileLog)
        ? Math.max(percentileLog, madThresholdLog)
        : madThresholdLog;
    const maximumWeightLog = robustWeightBaseLog
        + Math.log(CONFIG.advancedTradeThresholdUsdWeightCapMultiplier);
    const volumeWeightedLog = hasPercentileSupport
        ? calculateUsdWeightedLogQuantile(sortedLogs, params.volumePercentile, maximumWeightLog)
        : NaN;
    const clusterResult = calculateUpperTailClusterBoundary(
        sortedLogs,
        params.tailPercentile,
        params.minimumClusterSamples
    );
    const clusterThresholdLog = clusterResult.valid ? clusterResult.boundaryLog : NaN;

    // The production threshold is conservative: no individual estimator may pull it below the others.
    const candidates = [
        { name: "mad", logValue: madThresholdLog },
        { name: "count_percentile", logValue: percentileLog },
        { name: "usd_weighted_percentile", logValue: volumeWeightedLog },
        { name: "upper_tail_cluster", logValue: clusterThresholdLog }
    ].filter(item => Number.isFinite(item.logValue));
    let winningCandidate = null;
    for (const candidate of candidates) {
        if (!winningCandidate || candidate.logValue > winningCandidate.logValue) {
            winningCandidate = candidate;
        }
    }

    const thresholdLog = winningCandidate?.logValue;
    const candidateUsd = Number.isFinite(thresholdLog) ? Math.exp(thresholdLog) : NaN;
    const candidateValid = Number.isFinite(candidateUsd) && candidateUsd > 0;
    const estimator = candidateValid ? `hybrid_${winningCandidate.name}` : "none";

    return {
        valid: candidateValid,
        totalSeen,
        sampleLength,
        percentile: params.percentile,
        volumePercentile: params.volumePercentile,
        tailPercentile: params.tailPercentile,
        percentileLog,
        volumeWeightedLog,
        maximumWeightLog,
        medianLog,
        madLog,
        robustSigmaLog,
        madThresholdLog,
        clusterThresholdLog,
        clusterResult,
        thresholdLog,
        candidateUsd,
        estimator,
        reason: candidateValid ? (hasPercentileSupport ? "ok" : "mad_or_cluster_fallback") : "invalid_candidate",
        componentsUsd: {
            countPercentile: Number.isFinite(percentileLog) ? Math.exp(percentileLog) : null,
            mad: Number.isFinite(madThresholdLog) ? Math.exp(madThresholdLog) : null,
            usdWeightedPercentile: Number.isFinite(volumeWeightedLog) ? Math.exp(volumeWeightedLog) : null,
            upperTailCluster: Number.isFinite(clusterThresholdLog) ? Math.exp(clusterThresholdLog) : null
        }
    };
}

function calculateSignificantPercentileModel(sortedLogs, baseThreshold, params) {
    const sampleLength = sortedLogs.length;
    const percentile = clampNumber(params.advancedSignificantTradePercentile, 0.900, 0.998, 0.995);
    const minimumSamples = Math.max(50, params.advancedSignificantTradeMinimumSamples);
    const candidateLogRatio = sampleLength > 0 ? quantileFromSortedFloat64(sortedLogs, percentile) : NaN;
    const candidateRatio = Number.isFinite(candidateLogRatio) ? Math.exp(candidateLogRatio) : NaN;
    const candidateUsd = Number.isFinite(candidateRatio)
        ? Math.min(10000000, baseThreshold * candidateRatio)
        : null;
    const expectedTailObservations = sampleLength * Math.max(1e-6, 1 - percentile);
    const sampleSupport = Math.min(1, sampleLength / minimumSamples);
    const tailSupport = Math.min(1, expectedTailObservations / 2);
    const confidence = Math.max(0, Math.min(1, 0.45 * sampleSupport + 0.55 * Math.sqrt(tailSupport)));
    const valid = sampleLength >= minimumSamples
        && Number.isFinite(candidateRatio)
        && candidateRatio > 1;
    return {
        valid,
        ready: valid,
        reason: valid
            ? "percentile_ready"
            : sampleLength < minimumSamples
                ? "percentile_insufficient_samples"
                : "percentile_invalid_candidate",
        estimator: "upper_percentile",
        percentile,
        sampleLength,
        minimumSamples,
        expectedTailObservations,
        candidateRatio: Number.isFinite(candidateRatio) ? candidateRatio : null,
        candidateUsd,
        confidence
    };
}

function estimateGeneralizedParetoByLmoments(sortedExcesses) {
    const count = sortedExcesses.length;
    if (count < 3) return { valid: false, reason: "evt_insufficient_exceedances" };
    let sum = 0;
    let weightedSum = 0;
    const denominator = Math.max(1, count - 1);
    for (let index = 0; index < count; index += 1) {
        const value = sortedExcesses[index];
        if (!Number.isFinite(value) || value < 0) return { valid: false, reason: "evt_invalid_excesses" };
        sum += value;
        weightedSum += (index / denominator) * value;
    }
    const l1 = sum / count;
    const b1 = weightedSum / count;
    const l2 = 2 * b1 - l1;
    if (!Number.isFinite(l1) || !Number.isFinite(l2) || l1 <= Number.EPSILON || l2 <= Number.EPSILON) {
        return { valid: false, reason: "evt_degenerate_tail" };
    }
    const lCv = l2 / l1;
    if (!Number.isFinite(lCv) || lCv <= 0 || lCv >= 1) {
        return { valid: false, reason: "evt_unstable_fit" };
    }
    const shape = 2 - 1 / lCv;
    const scale = l1 * (1 - shape);
    const valid = Number.isFinite(shape)
        && Number.isFinite(scale)
        && shape > -0.45
        && shape < 0.95
        && scale > Number.EPSILON;
    return {
        valid,
        reason: valid ? "evt_fit_ready" : "evt_unstable_fit",
        shape,
        scale,
        l1,
        l2,
        lCv
    };
}

function findFirstSortedFloat64GreaterThan(sortedValues, target) {
    let low = 0;
    let high = sortedValues.length;
    while (low < high) {
        const middle = low + Math.floor((high - low) / 2);
        if (sortedValues[middle] <= target) low = middle + 1;
        else high = middle;
    }
    return low;
}

function calculateSignificantEvtModel(sortedLogs, baseThreshold, percentileModel, params) {
    const sampleLength = sortedLogs.length;
    const minimumSamples = Math.max(50, params.advancedSignificantTradeMinimumSamples);
    const thresholdPercentile = clampNumber(params.advancedSignificantTradeEvtThresholdPercentile, 0.70, 0.97, 0.90);
    const minimumExceedances = Math.max(10, params.advancedSignificantTradeEvtMinimumExceedances);
    const targetPercentile = clampNumber(params.advancedSignificantTradePercentile, 0.900, 0.998, 0.995);
    if (sampleLength < minimumSamples) {
        return {
            valid: false,
            ready: false,
            reason: "evt_insufficient_samples",
            estimator: "evt_gpd_lmoments",
            sampleLength,
            minimumSamples,
            thresholdPercentile,
            minimumExceedances,
            exceedanceCount: 0,
            targetPercentile,
            candidateRatio: null,
            candidateUsd: null,
            confidence: 0
        };
    }

    const thresholdLogRatio = quantileFromSortedFloat64(sortedLogs, thresholdPercentile);
    const thresholdRatio = Number.isFinite(thresholdLogRatio) ? Math.exp(thresholdLogRatio) : NaN;
    if (!Number.isFinite(thresholdRatio) || thresholdRatio <= 1) {
        return {
            valid: false,
            ready: false,
            reason: "evt_invalid_threshold",
            estimator: "evt_gpd_lmoments",
            sampleLength,
            minimumSamples,
            thresholdPercentile,
            minimumExceedances,
            exceedanceCount: 0,
            targetPercentile,
            thresholdRatio: Number.isFinite(thresholdRatio) ? thresholdRatio : null,
            candidateRatio: null,
            candidateUsd: null,
            confidence: 0
        };
    }

    const firstExceedanceIndex = findFirstSortedFloat64GreaterThan(sortedLogs, thresholdLogRatio);
    const maximumObservedRatio = Math.exp(sortedLogs[sampleLength - 1]);
    const exceedanceCount = Math.max(0, sampleLength - firstExceedanceIndex);
    const excesses = new Float64Array(exceedanceCount);
    for (let index = 0; index < exceedanceCount; index += 1) {
        excesses[index] = Math.max(0, Math.exp(sortedLogs[firstExceedanceIndex + index]) - thresholdRatio);
    }
    if (exceedanceCount < minimumExceedances) {
        return {
            valid: false,
            ready: false,
            reason: "evt_insufficient_exceedances",
            estimator: "evt_gpd_lmoments",
            sampleLength,
            minimumSamples,
            thresholdPercentile,
            thresholdRatio,
            thresholdUsd: Math.min(10000000, baseThreshold * thresholdRatio),
            minimumExceedances,
            exceedanceCount,
            targetPercentile,
            candidateRatio: null,
            candidateUsd: null,
            confidence: Math.min(1, exceedanceCount / minimumExceedances) * 0.5
        };
    }

    // sortedLogs is ascending, so the derived excesses are already sorted.
    const fit = estimateGeneralizedParetoByLmoments(excesses);
    if (!fit.valid) {
        return {
            valid: false,
            ready: false,
            reason: fit.reason || "evt_unstable_fit",
            estimator: "evt_gpd_lmoments",
            sampleLength,
            minimumSamples,
            thresholdPercentile,
            thresholdRatio,
            thresholdUsd: Math.min(10000000, baseThreshold * thresholdRatio),
            minimumExceedances,
            exceedanceCount,
            targetPercentile,
            shape: Number.isFinite(fit.shape) ? fit.shape : null,
            scaleRatio: Number.isFinite(fit.scale) ? fit.scale : null,
            candidateRatio: null,
            candidateUsd: null,
            confidence: 0
        };
    }

    const empiricalTailProbability = exceedanceCount / sampleLength;
    const targetTailProbability = Math.max(1e-6, 1 - targetPercentile);
    if (!(targetTailProbability < empiricalTailProbability)) {
        return {
            valid: false,
            ready: false,
            reason: "evt_target_not_beyond_threshold",
            estimator: "evt_gpd_lmoments",
            sampleLength,
            minimumSamples,
            thresholdPercentile,
            thresholdRatio,
            thresholdUsd: Math.min(10000000, baseThreshold * thresholdRatio),
            minimumExceedances,
            exceedanceCount,
            targetPercentile,
            empiricalTailProbability,
            targetTailProbability,
            shape: fit.shape,
            scaleRatio: fit.scale,
            candidateRatio: null,
            candidateUsd: null,
            confidence: 0
        };
    }

    const extrapolationFactor = empiricalTailProbability / targetTailProbability;
    let fittedExcess;
    if (Math.abs(fit.shape) < 1e-6) {
        fittedExcess = fit.scale * Math.log(extrapolationFactor);
    } else {
        fittedExcess = (fit.scale / fit.shape) * (Math.pow(extrapolationFactor, fit.shape) - 1);
    }
    const fittedCandidateRatio = thresholdRatio + fittedExcess;
    const percentileCandidateRatio = Number(percentileModel?.candidateRatio);
    const empiricalFloorRatio = Number.isFinite(percentileCandidateRatio) ? percentileCandidateRatio : 1;
    const uncappedCandidateRatio = Math.max(empiricalFloorRatio, fittedCandidateRatio);
    const maximumAllowedRatio = Math.max(maximumObservedRatio, empiricalFloorRatio) * 3;
    const capped = Number.isFinite(uncappedCandidateRatio) && uncappedCandidateRatio > maximumAllowedRatio;
    const candidateRatio = capped ? maximumAllowedRatio : uncappedCandidateRatio;
    const candidateUsd = Number.isFinite(candidateRatio)
        ? Math.min(10000000, baseThreshold * candidateRatio)
        : null;
    const sampleSupport = Math.min(1, sampleLength / minimumSamples);
    const peakSupport = Math.min(1, exceedanceCount / minimumExceedances);
    const shapeSupport = fit.shape <= 0.5 ? 1 : Math.max(0, (0.95 - fit.shape) / 0.45);
    const extrapolationSupport = extrapolationFactor <= 50
        ? 1
        : Math.max(0, 1 - (extrapolationFactor - 50) / 150);
    const confidence = Math.max(0, Math.min(1,
        0.25 * sampleSupport
        + 0.35 * peakSupport
        + 0.25 * shapeSupport
        + 0.15 * extrapolationSupport
    ));
    const valid = Number.isFinite(candidateRatio)
        && candidateRatio > thresholdRatio
        && Number.isFinite(candidateUsd)
        && candidateUsd > baseThreshold;
    return {
        valid,
        ready: valid,
        reason: valid ? (capped ? "evt_ready_capped" : "evt_ready") : "evt_invalid_candidate",
        estimator: "evt_gpd_lmoments",
        sampleLength,
        minimumSamples,
        thresholdPercentile,
        thresholdRatio,
        thresholdUsd: Math.min(10000000, baseThreshold * thresholdRatio),
        minimumExceedances,
        exceedanceCount,
        targetPercentile,
        empiricalTailProbability,
        targetTailProbability,
        extrapolationFactor,
        shape: fit.shape,
        scaleRatio: fit.scale,
        fittedCandidateRatio: Number.isFinite(fittedCandidateRatio) ? fittedCandidateRatio : null,
        candidateRatio: Number.isFinite(candidateRatio) ? candidateRatio : null,
        candidateUsd,
        maximumObservedRatio,
        capped,
        confidence
    };
}

function calculateSignificantTradeTailModels(relativeStrengthBuffer, bigTradesThresholdUsd, algorithmSettings, evaluatedAt = nowMs()) {
    const params = algorithmSettings || getCurrentAdvancedTradeThresholdAlgorithmSettings();
    const baseThreshold = clampNumber(bigTradesThresholdUsd, 100, 10000000, ADVANCED_TRADE_THRESHOLD_DEFAULTS.bigTradesThresholdUsd);
    const maximumAgeMs = params.advancedSignificantTradeHistoryMaxAgeMinutes * 60 * 1000;
    const sortedLogs = getRecentRelativeStrengthSamples(relativeStrengthBuffer, maximumAgeMs, evaluatedAt);
    sortedLogs.sort();
    const sampleLength = sortedLogs.length;
    const totalSeen = Math.max(0, Math.floor(Number(relativeStrengthBuffer?.seen) || 0));
    const percentileModel = calculateSignificantPercentileModel(sortedLogs, baseThreshold, params);
    const evtModel = calculateSignificantEvtModel(sortedLogs, baseThreshold, percentileModel, params);
    const requestedMethod = sanitizeSignificantTradeThresholdMethod(params.advancedSignificantTradeMethod);

    let appliedModel = null;
    let appliedMethod = requestedMethod;
    let fallbackUsed = false;
    let reason = "insufficient_significant_samples";
    if (requestedMethod === SIGNIFICANT_TRADE_THRESHOLD_METHOD.EVT) {
        if (evtModel.valid) {
            appliedModel = evtModel;
            appliedMethod = SIGNIFICANT_TRADE_THRESHOLD_METHOD.EVT;
            reason = "evt_selected";
        } else if (percentileModel.valid) {
            appliedModel = percentileModel;
            appliedMethod = SIGNIFICANT_TRADE_THRESHOLD_METHOD.PERCENTILE;
            fallbackUsed = true;
            reason = "evt_fallback_percentile";
        } else {
            reason = evtModel.reason || percentileModel.reason || "insufficient_significant_samples";
        }
    } else if (percentileModel.valid) {
        appliedModel = percentileModel;
        appliedMethod = SIGNIFICANT_TRADE_THRESHOLD_METHOD.PERCENTILE;
        reason = "percentile_selected";
    } else {
        reason = percentileModel.reason || "insufficient_significant_samples";
    }

    const maximumObservedRatio = sampleLength > 0 ? Math.exp(sortedLogs[sampleLength - 1]) : null;
    return {
        valid: Boolean(appliedModel?.valid),
        ready: Boolean(appliedModel?.valid),
        reason,
        estimator: appliedModel?.estimator || "none",
        requestedMethod,
        appliedMethod,
        fallbackUsed,
        sampleLength,
        totalSeen,
        baseThresholdUsd: baseThreshold,
        candidateRatio: Number.isFinite(Number(appliedModel?.candidateRatio)) ? Number(appliedModel.candidateRatio) : null,
        candidateUsd: Number.isFinite(Number(appliedModel?.candidateUsd)) ? Number(appliedModel.candidateUsd) : null,
        confidence: clampNumber(appliedModel?.confidence, 0, 1, 0),
        maximumObservedRatio: Number.isFinite(maximumObservedRatio) ? maximumObservedRatio : null,
        percentileModel,
        evtModel
    };
}

function smoothSignificantTradeThresholdUsd(previousValue, candidateValue, bigTradesThresholdUsd, significantResult, algorithmSettings) {
    const params = algorithmSettings || getCurrentAdvancedTradeThresholdAlgorithmSettings();
    const baseThreshold = clampNumber(bigTradesThresholdUsd, 100, 10000000, ADVANCED_TRADE_THRESHOLD_DEFAULTS.bigTradesThresholdUsd);
    const previousRatio = Math.max(1, clampNumber(previousValue, baseThreshold, 10000000, baseThreshold) / baseThreshold);
    const candidateRatio = Math.max(1, clampNumber(candidateValue, baseThreshold, 10000000, baseThreshold) / baseThreshold);
    const alpha = params.advancedSignificantTradeLogSmoothingAlpha;
    const smoothedRatio = Math.exp(Math.log(previousRatio) + alpha * (Math.log(candidateRatio) - Math.log(previousRatio)));
    const smoothedUsd = Math.min(10000000, baseThreshold * Math.max(1, smoothedRatio));
    const roundedUsd = roundAdvancedThresholdUsd(smoothedUsd);
    return clampNumber(Math.max(baseThreshold, roundedUsd), baseThreshold, 10000000, smoothedUsd);
}

function roundAdvancedThresholdUsd(value) {
    const safeValue = clampNumber(value, 100, 10000000, 100);
    let step = 10;
    if (safeValue >= 1000000) step = 10000;
    else if (safeValue >= 100000) step = 1000;
    else if (safeValue >= 10000) step = 500;
    else if (safeValue >= 1000) step = 100;
    return clampNumber(Math.round(safeValue / step) * step, 100, 10000000, safeValue);
}

function smoothAdvancedThresholdUsd(previousValue, candidateValue, algorithmSettings) {
    const previous = clampNumber(previousValue, 100, 10000000, 100);
    const candidate = clampNumber(candidateValue, 100, 10000000, previous);
    const params = algorithmSettings || getCurrentAdvancedTradeThresholdAlgorithmSettings();
    const alpha = params.advancedTradeThresholdLogSmoothingAlpha;
    const smoothedLog = Math.log(previous) + alpha * (Math.log(candidate) - Math.log(previous));
    const smoothed = Math.exp(smoothedLog);
    const lowerBound = Math.max(100, previous * params.advancedTradeThresholdMaxDownRatio);
    const upperBound = Math.min(10000000, previous * params.advancedTradeThresholdMaxUpRatio);
    const bounded = clampNumber(smoothed, lowerBound, upperBound, previous);
    const rounded = roundAdvancedThresholdUsd(bounded);
    return clampNumber(rounded, lowerBound, upperBound, bounded);
}

function getConfiguredAdvancedThresholdRuntime(settings = state.settings) {
    const scalping = settings?.scalping || {};
    const runtime = scalping.autoThresholdRuntime && typeof scalping.autoThresholdRuntime === "object"
        ? scalping.autoThresholdRuntime
        : {};
    const selectedMarket = getTokenMarket(state.selectedToken, settings?.activeMarket || DEFAULT_MARKET);
    const selectedSymbol = state.selectedToken ? getRequestSymbol(state.selectedToken) : String(runtime.requestSymbol || "").trim().toUpperCase();
    const bigTradesThresholdUsd = clampNumber(runtime.bigTradesThresholdUsd, 100, 10000000, ADVANCED_TRADE_THRESHOLD_DEFAULTS.bigTradesThresholdUsd);
    const legacySignificantThreshold = runtime.significantTradesThresholdUsd ?? runtime.bigBuysThresholdUsd;
    const significantTradesThresholdUsd = Math.max(
        bigTradesThresholdUsd,
        clampNumber(legacySignificantThreshold, 100, 10000000, ADVANCED_TRADE_THRESHOLD_DEFAULTS.significantTradesThresholdUsd)
    );
    const significantTradesReady = significantTradesThresholdUsd > bigTradesThresholdUsd;
    return {
        market: sanitizeMarket(runtime.market || selectedMarket, selectedMarket),
        requestSymbol: String(runtime.requestSymbol || selectedSymbol || "").trim().toUpperCase(),
        bigTradesThresholdUsd,
        significantTradesThresholdUsd,
        // A valid persisted threshold is an active fallback immediately after startup.
        // Confidence remains a separate indication of whether the adaptive model confirmed it.
        significantTradesReady,
        significantTradesConfidence: clampNumber(runtime.significantTradesConfidence, 0, 1, 0),
        updatedAt: Math.max(0, Math.floor(Number(runtime.updatedAt) || 0))
    };
}

function createAdvancedTradeThresholdRuntime() {
    const configured = getConfiguredAdvancedThresholdRuntime();
    return {
        generation: 0,
        timer: null,
        progressTimer: null,
        windowStartedAt: 0,
        allTrades: createLogNotionalReservoir(CONFIG.advancedTradeThresholdAllReservoirCapacity),
        significantLargeTrades: createTimedRelativeStrengthBuffer(CONFIG.advancedSignificantTradeHistoryCapacity),
        market: configured.market,
        requestSymbol: configured.requestSymbol,
        // Baselines are the pre-Turso fallback values supplied by the browser
        // (persisted local auto threshold or the existing default/manual fallback).
        configuredFallbackBigTradesThresholdUsd: configured.bigTradesThresholdUsd,
        configuredFallbackSignificantTradesThresholdUsd: configured.significantTradesThresholdUsd,
        bigTradesThresholdUsd: configured.bigTradesThresholdUsd,
        significantTradesThresholdUsd: configured.significantTradesThresholdUsd,
        // Readiness is session-local. Persisted/fallback values are active thresholds,
        // but they are not proof that the adaptive model has enough live samples.
        bigTradesAutoReady: false,
        significantTradesAutoReady: false,
        tursoFallbackLargeActive: false,
        tursoFallbackSignificantActive: false,
        significantTradesReady: configured.significantTradesReady,
        significantTradesConfidence: configured.significantTradesConfidence,
        updatedAt: configured.updatedAt,
        lastEvaluation: null,
        samplesRecorded: 0,
        significantSamplesRecorded: 0,
        lastSampleAt: 0,
        lastProgressPostedAt: 0
    };
}

function clearAdvancedTradeThresholdTimer(runtime) {
    if (!runtime?.timer) return;
    clearTimeout(runtime.timer);
    runtime.timer = null;
}

function clearAdvancedTradeThresholdProgressTimer(runtime) {
    if (!runtime?.progressTimer) return;
    clearInterval(runtime.progressTimer);
    runtime.progressTimer = null;
}

function resetAdvancedTradeThresholdWindow(options = {}) {
    const runtime = state.scalpingState?.advancedTradeThresholds;
    if (!runtime) return;
    const preserveSignificantTradeHistory = options.preserveSignificantTradeHistory === true;
    clearAdvancedTradeThresholdTimer(runtime);
    clearAdvancedTradeThresholdProgressTimer(runtime);
    runtime.generation += 1;
    runtime.windowStartedAt = 0;
    resetLogNotionalReservoir(runtime.allTrades);
    if (!preserveSignificantTradeHistory) {
        resetTimedRelativeStrengthBuffer(runtime.significantLargeTrades);
        runtime.significantSamplesRecorded = 0;
    }
    runtime.lastEvaluation = null;
    if (options.reloadConfiguredThresholds === true) {
        const configured = getConfiguredAdvancedThresholdRuntime();
        runtime.market = configured.market;
        runtime.requestSymbol = configured.requestSymbol;
        runtime.configuredFallbackBigTradesThresholdUsd = configured.bigTradesThresholdUsd;
        runtime.configuredFallbackSignificantTradesThresholdUsd = configured.significantTradesThresholdUsd;
        if (configured.updatedAt >= runtime.updatedAt) {
            if (!runtime.tursoFallbackLargeActive || runtime.bigTradesAutoReady) {
                runtime.bigTradesThresholdUsd = configured.bigTradesThresholdUsd;
            }
            if (!runtime.tursoFallbackSignificantActive || runtime.significantTradesAutoReady) {
                runtime.significantTradesThresholdUsd = Math.max(
                    runtime.bigTradesThresholdUsd,
                    configured.significantTradesThresholdUsd
                );
            }
            runtime.significantTradesReady = runtime.significantTradesThresholdUsd > runtime.bigTradesThresholdUsd;
            runtime.significantTradesConfidence = configured.significantTradesConfidence;
            runtime.updatedAt = configured.updatedAt;
        }
    }
}

function evaluateOneSecondBigTradeQualification(side, tradeNotional, effectiveThresholds) {
    const normalizedSide = side === "sell" ? "sell" : side === "buy" ? "buy" : "unknown";
    const notional = Number(tradeNotional);
    const bigTradesThresholdUsd = Number(effectiveThresholds?.bigTradesThresholdUsd);
    const significantTradesThresholdUsd = Number(effectiveThresholds?.significantTradesThresholdUsd);
    if (
        normalizedSide === "unknown"
        || !Number.isFinite(notional)
        || notional <= 0
        || !Number.isFinite(bigTradesThresholdUsd)
        || bigTradesThresholdUsd <= 0
        || !Number.isFinite(significantTradesThresholdUsd)
        || significantTradesThresholdUsd <= 0
    ) {
        return {
            isBigTrade: false,
            isSignificantTrade: false,
            shouldRecordMinuteBuy: false,
            shouldRecordMinuteSell: false,
            significantThresholdUsd: 0
        };
    }

    const isBigTrade = notional >= bigTradesThresholdUsd;
    const hasActiveSignificantThreshold = significantTradesThresholdUsd > bigTradesThresholdUsd;
    const isSignificantTrade = hasActiveSignificantThreshold
        && isBigTrade
        && notional >= significantTradesThresholdUsd;
    return {
        isBigTrade,
        isSignificantTrade,
        shouldRecordMinuteBuy: normalizedSide === "buy" && isSignificantTrade,
        shouldRecordMinuteSell: normalizedSide === "sell" && isSignificantTrade,
        significantThresholdUsd: Math.max(bigTradesThresholdUsd, significantTradesThresholdUsd)
    };
}

function getEffectiveTradeThresholds() {
    const scalping = state.settings.scalping || DEFAULT_SCALPING_SETTINGS;
    const manualBigTrades = clampNumber(scalping.bigTradesThresholdUsd, 100, 10000000, DEFAULT_SCALPING_SETTINGS.bigTradesThresholdUsd);
    const manualSignificant = Math.max(
        manualBigTrades,
        clampNumber(scalping.significantTradesThresholdUsd ?? scalping.bigBuysThresholdUsd, 100, 10000000, DEFAULT_SCALPING_SETTINGS.significantTradesThresholdUsd)
    );
    const runtime = state.scalpingState?.advancedTradeThresholds;
    if (scalping.advancedTradeThresholdsEnabled === true && runtime) {
        const bigTradesThresholdUsd = clampNumber(runtime.bigTradesThresholdUsd, 100, 10000000, ADVANCED_TRADE_THRESHOLD_DEFAULTS.bigTradesThresholdUsd);
        const significantTradesThresholdUsd = Math.max(
            bigTradesThresholdUsd,
            clampNumber(runtime.significantTradesThresholdUsd, 100, 10000000, ADVANCED_TRADE_THRESHOLD_DEFAULTS.significantTradesThresholdUsd)
        );
        return {
            source: "advanced",
            bigTradesThresholdUsd,
            significantTradesThresholdUsd,
            significantTradesReady: significantTradesThresholdUsd > bigTradesThresholdUsd,
            significantTradesConfidence: clampNumber(runtime.significantTradesConfidence, 0, 1, 0)
        };
    }
    return {
        source: "manual",
        bigTradesThresholdUsd: manualBigTrades,
        significantTradesThresholdUsd: manualSignificant,
        significantTradesReady: true,
        significantTradesConfidence: 1
    };
}

function buildAdvancedTradeThresholdProgressPayload(runtime, reason = "progress") {
    const evaluatedAt = nowMs();
    const algorithmSettings = getCurrentAdvancedTradeThresholdAlgorithmSettings();
    // Live diagnostics use the same production estimators but never mutate thresholds.
    const tradeResult = calculateHybridLogThreshold(runtime.allTrades, {
        percentile: algorithmSettings.advancedTradeThresholdPercentile,
        volumePercentile: algorithmSettings.advancedTradeThresholdVolumePercentile,
        tailPercentile: algorithmSettings.advancedTradeThresholdTailPercentile,
        madMultiplier: algorithmSettings.advancedTradeThresholdMadMultiplier,
        minimumSamples: CONFIG.advancedTradeThresholdMinAllSamples,
        minimumPercentileSamples: CONFIG.advancedTradeThresholdMinAllPercentileSamples,
        minimumClusterSamples: CONFIG.advancedTradeThresholdMinAllClusterSamples
    });
    const significantResult = calculateSignificantTradeTailModels(
        runtime.significantLargeTrades,
        runtime.bigTradesThresholdUsd,
        algorithmSettings,
        evaluatedAt
    );
    const percentileModel = significantResult.percentileModel || {};
    const evtModel = significantResult.evtModel || {};
    return {
        method: ADVANCED_TRADE_THRESHOLD_METHOD,
        market: getTokenMarket(state.selectedToken, state.settings.activeMarket),
        requestSymbol: state.selectedToken ? getRequestSymbol(state.selectedToken) : runtime.requestSymbol,
        windowActive: runtime.windowStartedAt > 0,
        windowStartedAt: runtime.windowStartedAt,
        windowMs: CONFIG.advancedTradeThresholdWindowMs,
        nextEvaluationAt: runtime.windowStartedAt > 0
            ? runtime.windowStartedAt + CONFIG.advancedTradeThresholdWindowMs
            : 0,
        allTradeCount: Math.max(0, Math.floor(Number(runtime.allTrades?.seen) || 0)),
        allTradeSampleUsed: Math.max(0, Math.floor(Number(tradeResult.sampleLength) || 0)),
        minimumAllSamples: CONFIG.advancedTradeThresholdMinAllSamples,
        minimumAllPercentileSamples: CONFIG.advancedTradeThresholdMinAllPercentileSamples,
        tradePreviewReady: tradeResult.valid === true,
        bigTradesAutoReady: runtime.bigTradesAutoReady === true,
        significantTradesAutoReady: runtime.significantTradesAutoReady === true,
        tradePreviewCandidateUsd: tradeResult.valid && Number.isFinite(Number(tradeResult.candidateUsd))
            ? roundAdvancedThresholdUsd(tradeResult.candidateUsd)
            : null,
        tradePreviewEstimator: tradeResult.estimator || "none",
        tradePreviewReason: tradeResult.reason || "waiting",
        tradePreviewComponentsUsd: tradeResult.componentsUsd || null,
        significantTradeCount: Math.max(0, Math.floor(Number(significantResult.sampleLength) || 0)),
        significantTradeTotalSeen: Math.max(0, Math.floor(Number(runtime.significantLargeTrades?.seen) || 0)),
        minimumSignificantSamples: algorithmSettings.advancedSignificantTradeMinimumSamples,
        significantMethodRequested: significantResult.requestedMethod,
        significantMethodApplied: significantResult.appliedMethod,
        significantFallbackUsed: significantResult.fallbackUsed === true,
        percentileLevel: percentileModel.percentile,
        percentileReady: percentileModel.valid === true,
        percentileCandidateUsd: Number.isFinite(Number(percentileModel.candidateUsd))
            ? roundAdvancedThresholdUsd(percentileModel.candidateUsd)
            : null,
        percentileConfidence: clampNumber(percentileModel.confidence, 0, 1, 0),
        percentileReason: percentileModel.reason || "waiting",
        evtThresholdPercentile: evtModel.thresholdPercentile,
        evtThresholdUsd: Number.isFinite(Number(evtModel.thresholdUsd))
            ? roundAdvancedThresholdUsd(evtModel.thresholdUsd)
            : null,
        evtExceedanceCount: Math.max(0, Math.floor(Number(evtModel.exceedanceCount) || 0)),
        evtMinimumExceedances: Math.max(1, Math.floor(Number(evtModel.minimumExceedances) || algorithmSettings.advancedSignificantTradeEvtMinimumExceedances)),
        evtShape: Number.isFinite(Number(evtModel.shape)) ? Number(evtModel.shape) : null,
        evtScaleRatio: Number.isFinite(Number(evtModel.scaleRatio)) ? Number(evtModel.scaleRatio) : null,
        evtReady: evtModel.valid === true,
        evtCandidateUsd: Number.isFinite(Number(evtModel.candidateUsd))
            ? roundAdvancedThresholdUsd(evtModel.candidateUsd)
            : null,
        evtConfidence: clampNumber(evtModel.confidence, 0, 1, 0),
        evtReason: evtModel.reason || "waiting",
        candidateUsd: Number.isFinite(Number(significantResult.candidateUsd)) && Number(significantResult.candidateUsd) > 0
            ? roundAdvancedThresholdUsd(significantResult.candidateUsd)
            : null,
        previewConfidence: clampNumber(significantResult.confidence, 0, 1, 0),
        modelReason: significantResult.reason || "waiting",
        significantTradesThresholdUsd: runtime.significantTradesThresholdUsd,
        significantTradesConfidence: runtime.significantTradesConfidence,
        progressAt: evaluatedAt,
        reason,
        significantModel: significantResult
    };
}

function postAdvancedTradeThresholdProgress(runtime, reason = "progress") {
    const scalping = state.settings.scalping || {};
    if (!runtime || scalping.enabled !== true || scalping.advancedTradeThresholdsEnabled !== true) return;
    const payload = buildAdvancedTradeThresholdProgressPayload(runtime, reason);
    runtime.lastProgressPostedAt = payload.progressAt;
    post("AUTO_TRADE_THRESHOLDS_PROGRESS", payload);
}

function scheduleAdvancedTradeThresholdProgress(runtime) {
    if (!runtime || runtime.progressTimer || runtime.windowStartedAt <= 0) return;
    const generation = runtime.generation;
    runtime.progressTimer = setInterval(() => {
        if (generation !== runtime.generation || runtime.windowStartedAt <= 0) {
            clearAdvancedTradeThresholdProgressTimer(runtime);
            return;
        }
        postAdvancedTradeThresholdProgress(runtime, "interval");
    }, CONFIG.advancedTradeThresholdProgressIntervalMs);
}

function scheduleAdvancedTradeThresholdWindow(runtime) {
    if (!runtime || runtime.timer || runtime.windowStartedAt <= 0) return;
    const generation = runtime.generation;
    runtime.timer = setTimeout(() => {
        runtime.timer = null;
        if (generation !== runtime.generation) return;
        finalizeAdvancedTradeThresholdWindow("timer");
    }, CONFIG.advancedTradeThresholdWindowMs);
}

function recordAdvancedTradeThresholdSample(tradeNotional, effectiveThresholds = null, timestamp = nowMs()) {
    const scalping = state.settings.scalping || {};
    if (scalping.enabled !== true || scalping.advancedTradeThresholdsEnabled !== true) return;
    const runtime = state.scalpingState?.advancedTradeThresholds;
    if (!runtime || !Number.isFinite(tradeNotional) || tradeNotional <= 0) return;
    let windowStartedNow = false;
    if (runtime.windowStartedAt <= 0) {
        runtime.windowStartedAt = nowMs();
        runtime.generation += 1;
        windowStartedNow = true;
        scheduleAdvancedTradeThresholdWindow(runtime);
        scheduleAdvancedTradeThresholdProgress(runtime);
    }
    addLogNotionalToReservoir(runtime.allTrades, Math.log(tradeNotional));
    const thresholds = effectiveThresholds || getEffectiveTradeThresholds();
    const activeBigThreshold = clampNumber(thresholds.bigTradesThresholdUsd, 100, 10000000, runtime.bigTradesThresholdUsd);
    if (tradeNotional >= activeBigThreshold) {
        const logRelativeStrength = Math.log(tradeNotional / activeBigThreshold);
        if (addTimedRelativeStrengthSample(runtime.significantLargeTrades, logRelativeStrength, timestamp)) {
            runtime.significantSamplesRecorded += 1;
        }
    }
    runtime.samplesRecorded += 1;
    runtime.lastSampleAt = nowMs();
    if (windowStartedNow) postAdvancedTradeThresholdProgress(runtime, "window_started");
}

function finalizeAdvancedTradeThresholdWindow(reason = "timer") {
    const scalping = state.settings.scalping || {};
    const runtime = state.scalpingState?.advancedTradeThresholds;
    if (!runtime || scalping.enabled !== true || scalping.advancedTradeThresholdsEnabled !== true) {
        resetAdvancedTradeThresholdWindow();
        return;
    }

    const startedAt = runtime.windowStartedAt;
    const evaluatedAt = nowMs();
    const algorithmSettings = getCurrentAdvancedTradeThresholdAlgorithmSettings();
    const tradeResult = calculateHybridLogThreshold(runtime.allTrades, {
        percentile: algorithmSettings.advancedTradeThresholdPercentile,
        volumePercentile: algorithmSettings.advancedTradeThresholdVolumePercentile,
        tailPercentile: algorithmSettings.advancedTradeThresholdTailPercentile,
        madMultiplier: algorithmSettings.advancedTradeThresholdMadMultiplier,
        minimumSamples: CONFIG.advancedTradeThresholdMinAllSamples,
        minimumPercentileSamples: CONFIG.advancedTradeThresholdMinAllPercentileSamples,
        minimumClusterSamples: CONFIG.advancedTradeThresholdMinAllClusterSamples
    });

    let nextBigTrades = runtime.bigTradesThresholdUsd;
    let tradeUpdated = false;
    if (tradeResult.valid) {
        nextBigTrades = smoothAdvancedThresholdUsd(runtime.bigTradesThresholdUsd, tradeResult.candidateUsd, algorithmSettings);
        tradeUpdated = true;
        runtime.bigTradesAutoReady = true;
        runtime.tursoFallbackLargeActive = false;
    }

    const significantResult = calculateSignificantTradeTailModels(
        runtime.significantLargeTrades,
        nextBigTrades,
        algorithmSettings,
        evaluatedAt
    );
    let nextSignificantTrades = Math.max(nextBigTrades, runtime.significantTradesThresholdUsd);
    let significantUpdated = false;
    let significantConfidence = clampNumber(runtime.significantTradesConfidence, 0, 1, 0);
    if (significantResult.valid) {
        nextSignificantTrades = smoothSignificantTradeThresholdUsd(
            runtime.significantTradesThresholdUsd,
            significantResult.candidateUsd,
            nextBigTrades,
            significantResult,
            algorithmSettings
        );
        significantUpdated = true;
        runtime.significantTradesAutoReady = true;
        runtime.tursoFallbackSignificantActive = false;
        significantConfidence = clampNumber(significantResult.confidence, 0, 1, significantConfidence);
    }

    nextSignificantTrades = Math.max(nextBigTrades, nextSignificantTrades);
    const significantReady = nextSignificantTrades > nextBigTrades;
    const readinessChanged = runtime.significantTradesReady !== significantReady;
    const confidenceChanged = Math.abs(runtime.significantTradesConfidence - significantConfidence) >= 0.005;
    const updated = tradeUpdated || significantUpdated || readinessChanged;
    if (updated) {
        runtime.bigTradesThresholdUsd = nextBigTrades;
        runtime.significantTradesThresholdUsd = nextSignificantTrades;
        runtime.significantTradesReady = significantReady;
        runtime.updatedAt = evaluatedAt;
    }
    if (significantUpdated || confidenceChanged) runtime.significantTradesConfidence = significantConfidence;
    runtime.lastEvaluation = {
        reason,
        startedAt,
        evaluatedAt,
        updated,
        tradeResult,
        significantResult
    };

    post("AUTO_TRADE_THRESHOLDS_UPDATE", {
        method: ADVANCED_TRADE_THRESHOLD_METHOD,
        market: getTokenMarket(state.selectedToken, state.settings.activeMarket),
        requestSymbol: state.selectedToken ? getRequestSymbol(state.selectedToken) : runtime.requestSymbol,
        bigTradesThresholdUsd: runtime.bigTradesThresholdUsd,
        significantTradesThresholdUsd: runtime.significantTradesThresholdUsd,
        bigTradesAutoReady: runtime.bigTradesAutoReady === true,
        significantTradesAutoReady: runtime.significantTradesAutoReady === true,
        significantTradesReady: runtime.significantTradesReady,
        significantTradesConfidence: runtime.significantTradesConfidence,
        updatedAt: runtime.updatedAt,
        evaluatedAt,
        windowActive: false,
        windowStartedAt: startedAt,
        windowMs: CONFIG.advancedTradeThresholdWindowMs,
        nextEvaluationAt: 0,
        minimumSignificantSamples: algorithmSettings.advancedSignificantTradeMinimumSamples,
        significantMethodRequested: significantResult.requestedMethod,
        significantMethodApplied: significantResult.appliedMethod,
        significantFallbackUsed: significantResult.fallbackUsed === true,
        percentileLevel: significantResult.percentileModel?.percentile,
        percentileReady: significantResult.percentileModel?.valid === true,
        percentileCandidateUsd: Number.isFinite(Number(significantResult.percentileModel?.candidateUsd))
            ? roundAdvancedThresholdUsd(significantResult.percentileModel.candidateUsd)
            : null,
        percentileConfidence: clampNumber(significantResult.percentileModel?.confidence, 0, 1, 0),
        percentileReason: significantResult.percentileModel?.reason || "waiting",
        evtThresholdPercentile: significantResult.evtModel?.thresholdPercentile,
        evtThresholdUsd: Number.isFinite(Number(significantResult.evtModel?.thresholdUsd))
            ? roundAdvancedThresholdUsd(significantResult.evtModel.thresholdUsd)
            : null,
        evtExceedanceCount: Math.max(0, Math.floor(Number(significantResult.evtModel?.exceedanceCount) || 0)),
        evtMinimumExceedances: Math.max(1, Math.floor(Number(significantResult.evtModel?.minimumExceedances) || algorithmSettings.advancedSignificantTradeEvtMinimumExceedances)),
        evtShape: Number.isFinite(Number(significantResult.evtModel?.shape)) ? Number(significantResult.evtModel.shape) : null,
        evtScaleRatio: Number.isFinite(Number(significantResult.evtModel?.scaleRatio)) ? Number(significantResult.evtModel.scaleRatio) : null,
        evtReady: significantResult.evtModel?.valid === true,
        evtCandidateUsd: Number.isFinite(Number(significantResult.evtModel?.candidateUsd))
            ? roundAdvancedThresholdUsd(significantResult.evtModel.candidateUsd)
            : null,
        evtConfidence: clampNumber(significantResult.evtModel?.confidence, 0, 1, 0),
        evtReason: significantResult.evtModel?.reason || "waiting",
        previewConfidence: clampNumber(significantResult.confidence, 0, 1, 0),
        candidateUsd: Number.isFinite(Number(significantResult.candidateUsd)) && Number(significantResult.candidateUsd) > 0
            ? roundAdvancedThresholdUsd(significantResult.candidateUsd)
            : null,
        modelReason: significantResult.reason || "waiting",
        progressAt: evaluatedAt,
        updated,
        status: updated
            ? "updated"
            : !tradeResult.valid
                ? "insufficient_samples"
                : significantResult.valid !== true
                    ? "significant_warmup"
                    : "significant_unconfirmed",
        allTradeCount: tradeResult.totalSeen,
        allTradeSampleUsed: tradeResult.sampleLength,
        minimumAllSamples: CONFIG.advancedTradeThresholdMinAllSamples,
        minimumAllPercentileSamples: CONFIG.advancedTradeThresholdMinAllPercentileSamples,
        tradePreviewReady: tradeResult.valid === true,
        tradePreviewCandidateUsd: tradeResult.valid ? roundAdvancedThresholdUsd(tradeResult.candidateUsd) : null,
        tradePreviewEstimator: tradeResult.estimator || "none",
        tradePreviewReason: tradeResult.reason || "waiting",
        tradePreviewComponentsUsd: tradeResult.componentsUsd || null,
        significantTradeCount: significantResult.totalSeen || 0,
        significantTradeSampleUsed: significantResult.sampleLength || 0,
        significantTradeTotalSeen: significantResult.totalSeen || 0,
        tradeCandidateUsd: tradeResult.valid ? roundAdvancedThresholdUsd(tradeResult.candidateUsd) : null,
        significantCandidateUsd: significantResult.valid ? roundAdvancedThresholdUsd(significantResult.candidateUsd) : null,
        tradeEstimator: tradeResult.estimator || "none",
        significantEstimator: significantResult.estimator || "none",
        sampleSource: "aggTrade",
        streamDriven: true,
        samplesRecorded: runtime.samplesRecorded,
        significantSamplesRecorded: runtime.significantSamplesRecorded,
        lastSampleAt: runtime.lastSampleAt,
        tradeComponentsUsd: tradeResult.componentsUsd || null,
        tradeCluster: tradeResult.clusterResult || null,
        significantModel: significantResult,
        algorithmSettings
    });

    clearAdvancedTradeThresholdTimer(runtime);
    clearAdvancedTradeThresholdProgressTimer(runtime);
    runtime.generation += 1;
    runtime.windowStartedAt = 0;
    resetLogNotionalReservoir(runtime.allTrades);
    reconcileTursoThresholdFallbackSchedule("auto_threshold_evaluated");
}


function isMinuteTradeSignalHistoryEnabled(settings = state.settings) {
    return settings?.scalping?.enabled === true;
}

function isBigBuyMinuteSignalRecordingEnabled(settings = state.settings) {
    const scalping = settings?.scalping || {};
    return scalping.enabled === true && scalping.bigTradesEnabled !== false;
}

function isBigSellMinuteSignalRecordingEnabled(settings = state.settings) {
    const scalping = settings?.scalping || {};
    return scalping.enabled === true && scalping.bigTradesEnabled !== false;
}

function getBigBuySignalDatabaseIdentity(token = state.selectedToken) {
    if (!token) return null;
    const market = getTokenMarket(token, state.settings.activeMarket || DEFAULT_MARKET);
    const requestSymbol = String(getRequestSymbol(token) || "").trim().toUpperCase();
    if (!requestSymbol) return null;
    const symbol = String(token?.symbol || requestSymbol).trim().toUpperCase() || requestSymbol;
    return {
        market,
        requestSymbol,
        symbol,
        key: `${market}:${requestSymbol}`
    };
}

function getBigBuySignalDatabaseRange(referenceMs = nowMs()) {
    const safeReferenceMs = Number.isFinite(Number(referenceMs)) ? Number(referenceMs) : nowMs();
    const currentMinuteTime = Math.floor(safeReferenceMs / 60000) * BIG_BUY_SIGNAL_DATABASE.timeframeSeconds;
    return {
        currentMinuteTime,
        minimumTime: currentMinuteTime - BIG_BUY_SIGNAL_DATABASE.maxAgeCandles * BIG_BUY_SIGNAL_DATABASE.timeframeSeconds
    };
}

function reportBigBuySignalDatabaseError(operation, error) {
    const runtime = state.bigBuySignalDatabase;
    const timestamp = nowMs();
    if (timestamp - runtime.lastErrorAt < CONFIG.bigBuySignalDatabaseRetryDelayMs) return;
    runtime.lastErrorAt = timestamp;
    console.warn(`[WORKER-DB] ${operation} failed:`, error);
}

function indexedDbRequestToPromise(request, options = {}) {
    return new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = event => {
            if (options.ignoreConstraintError === true && request.error?.name === "ConstraintError") {
                event.preventDefault();
                event.stopPropagation();
                resolve(options.constraintResult);
                return;
            }
            reject(request.error || new Error("IndexedDB request failed"));
        };
    });
}

function indexedDbTransactionToPromise(transaction) {
    return new Promise((resolve, reject) => {
        transaction.oncomplete = () => resolve();
        transaction.onabort = () => reject(transaction.error || new Error("IndexedDB transaction aborted"));
        transaction.onerror = () => reject(transaction.error || new Error("IndexedDB transaction failed"));
    });
}

function openBigBuySignalDatabase() {
    const runtime = state.bigBuySignalDatabase;
    if (runtime.connection) return Promise.resolve(runtime.connection);
    if (runtime.openPromise) return runtime.openPromise;
    if (runtime.retryAfter > nowMs()) return Promise.resolve(null);
    if (typeof indexedDB === "undefined" || typeof IDBKeyRange === "undefined") {
        runtime.retryAfter = Number.MAX_SAFE_INTEGER;
        return Promise.resolve(null);
    }

    runtime.openPromise = new Promise(resolve => {
        let settled = false;
        const finish = database => {
            if (settled) {
                if (database && database !== runtime.connection) database.close();
                return;
            }
            settled = true;
            runtime.openPromise = null;
            resolve(database || null);
        };

        let request;
        try {
            request = indexedDB.open(BIG_BUY_SIGNAL_DATABASE.name, BIG_BUY_SIGNAL_DATABASE.version);
        } catch (error) {
            runtime.retryAfter = nowMs() + CONFIG.bigBuySignalDatabaseRetryDelayMs;
            reportBigBuySignalDatabaseError("open", error);
            finish(null);
            return;
        }

        request.onupgradeneeded = event => {
            const database = request.result;
            const transaction = request.transaction;
            let store;
            if (!database.objectStoreNames.contains(BIG_BUY_SIGNAL_DATABASE.storeName)) {
                store = database.createObjectStore(BIG_BUY_SIGNAL_DATABASE.storeName, { keyPath: "id" });
            } else {
                store = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.storeName);
            }
            // Preserve all historical minute signals across schema upgrades.
            // The index is rebuilt below without deleting the object-store records.
            if (store.indexNames.contains(BIG_BUY_SIGNAL_DATABASE.marketSymbolTimeIndex)) {
                store.deleteIndex(BIG_BUY_SIGNAL_DATABASE.marketSymbolTimeIndex);
            }
            store.createIndex(
                BIG_BUY_SIGNAL_DATABASE.marketSymbolTimeIndex,
                ["market", "requestSymbol", "signalType", "time"],
                { unique: true }
            );
            if (!store.indexNames.contains(BIG_BUY_SIGNAL_DATABASE.timeIndex)) {
                store.createIndex(BIG_BUY_SIGNAL_DATABASE.timeIndex, "time", { unique: false });
            }
            let outboxStore;
            if (!database.objectStoreNames.contains(BIG_BUY_SIGNAL_DATABASE.outboxStoreName)) {
                outboxStore = database.createObjectStore(BIG_BUY_SIGNAL_DATABASE.outboxStoreName, { keyPath: "id" });
            } else {
                outboxStore = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.outboxStoreName);
            }
            if (!outboxStore.indexNames.contains(BIG_BUY_SIGNAL_DATABASE.outboxQueuedAtIndex)) {
                outboxStore.createIndex(BIG_BUY_SIGNAL_DATABASE.outboxQueuedAtIndex, "queuedAt", { unique: false });
            }
        };
        request.onsuccess = () => {
            const database = request.result;
            runtime.connection = database;
            runtime.retryAfter = 0;
            database.onversionchange = () => {
                database.close();
                if (runtime.connection === database) runtime.connection = null;
                runtime.openPromise = null;
            };
            database.onclose = () => {
                if (runtime.connection === database) runtime.connection = null;
            };
            finish(database);
        };
        request.onerror = () => {
            runtime.retryAfter = nowMs() + CONFIG.bigBuySignalDatabaseRetryDelayMs;
            reportBigBuySignalDatabaseError("open", request.error);
            finish(null);
        };
        request.onblocked = () => {
            runtime.retryAfter = nowMs() + CONFIG.bigBuySignalDatabaseRetryDelayMs;
            reportBigBuySignalDatabaseError("open_blocked", new Error("IndexedDB upgrade is blocked by another page"));
            finish(null);
        };
    });

    return runtime.openPromise;
}

function getMinuteTradeSignalType(side) {
    return side === "sell" ? BIG_BUY_SIGNAL_DATABASE.sellSignalType : BIG_BUY_SIGNAL_DATABASE.buySignalType;
}

function createBigBuySignalDatabaseRecord(identity, signal, side = "buy") {
    const normalizedSide = side === "sell" ? "sell" : "buy";
    const signalType = getMinuteTradeSignalType(normalizedSide);
    const minuteTime = Math.floor(Number(signal?.time) || 0);
    const price = Number(signal?.price);
    const quantity = Number(signal?.qty);
    const notional = Number(signal?.notional);
    if (!identity || minuteTime <= 0 || !Number.isFinite(price) || price <= 0 || !Number.isFinite(notional) || notional <= 0) {
        return null;
    }
    return {
        id: `${identity.market}:${identity.requestSymbol}:${signalType}:${minuteTime}`,
        market: identity.market,
        symbol: identity.symbol,
        requestSymbol: identity.requestSymbol,
        time: minuteTime,
        tradeTime: Math.max(minuteTime, Math.floor(Number(signal?.tradeTime) || minuteTime)),
        price,
        qty: Number.isFinite(quantity) && quantity > 0 ? quantity : 0,
        notional,
        side: normalizedSide,
        signalType,
        label: signalType,
        createdAt: nowMs()
    };
}

async function persistBigBuyMinuteSignalToDatabase(identity, signal, side = "buy") {
    const record = createBigBuySignalDatabaseRecord(identity, signal, side);
    if (!record) return { inserted: false, duplicate: false, available: true, queuedForCloud: false };
    const database = await openBigBuySignalDatabase();
    if (!database) return { inserted: false, duplicate: false, available: false, queuedForCloud: false };

    const queueForCloud = isTursoSyncConfigured();
    try {
        return await new Promise((resolve, reject) => {
            const storeNames = queueForCloud
                ? [BIG_BUY_SIGNAL_DATABASE.storeName, BIG_BUY_SIGNAL_DATABASE.outboxStoreName]
                : [BIG_BUY_SIGNAL_DATABASE.storeName];
            const transaction = database.transaction(storeNames, "readwrite");
            let inserted = false;
            let duplicate = false;
            let queuedForCloud = false;
            let settled = false;
            const finish = (error = null) => {
                if (settled) return;
                settled = true;
                if (error) reject(error);
                else resolve({ inserted, duplicate, available: true, queuedForCloud });
            };
            transaction.oncomplete = () => finish();
            transaction.onabort = () => finish(transaction.error || new Error("IndexedDB signal transaction aborted"));
            transaction.onerror = () => {};

            const addRequest = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.storeName).add(record);
            addRequest.onsuccess = () => {
                inserted = true;
                if (!queueForCloud) return;
                const outboxRecord = { ...record, queuedAt: nowMs(), attemptCount: 0 };
                const outboxRequest = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.outboxStoreName).put(outboxRecord);
                outboxRequest.onsuccess = () => { queuedForCloud = true; };
                outboxRequest.onerror = () => {
                    try { transaction.abort(); } catch (error) {}
                };
            };
            addRequest.onerror = event => {
                if (addRequest.error?.name === "ConstraintError") {
                    event.preventDefault();
                    event.stopPropagation();
                    duplicate = true;
                    return;
                }
                try { transaction.abort(); } catch (error) {}
            };
        });
    } catch (error) {
        reportBigBuySignalDatabaseError(`save_${record.signalType}_signal`, error);
        return { inserted: false, duplicate: false, available: true, queuedForCloud: false };
    }
}

function createTursoArgument(value) {
    if (value === null || value === undefined) return { type: "null" };
    if (typeof value === "number") {
        if (!Number.isFinite(value)) return { type: "null" };
        return Number.isInteger(value)
            ? { type: "integer", value: String(value) }
            : { type: "float", value };
    }
    return { type: "text", value: String(value) };
}

function createTursoExecuteRequest(sql, args = []) {
    return {
        type: "execute",
        stmt: {
            sql: String(sql || ""),
            args: Array.isArray(args) ? args.map(createTursoArgument) : []
        }
    };
}

function extractTursoPipelineResults(payload) {
    if (!payload || !Array.isArray(payload.results)) throw new Error("Некоректна відповідь Turso SQL over HTTP");
    const results = [];
    for (const item of payload.results) {
        if (item?.type === "error") {
            const detail = item.error?.message || item.error?.code || "Turso statement failed";
            const error = new Error(String(detail));
            error.code = item.error?.code || "TURSO_STATEMENT_ERROR";
            throw error;
        }
        if (item?.response?.type === "execute") results.push(item.response.result || { cols: [], rows: [] });
    }
    return results;
}

function shouldRetryTursoError(error) {
    const status = Number(error?.status) || 0;
    const text = String(error?.message || "").toLowerCase();
    return status === 408 || status === 409 || status === 425 || status === 429 || status >= 500
        || text.includes("busy") || text.includes("conflict") || text.includes("locked")
        || text.includes("timeout") || text.includes("network") || text.includes("fetch");
}

function waitForTursoRetry(attempt) {
    const delay = CONFIG.tursoRetryBaseDelayMs * Math.pow(2, Math.max(0, attempt - 1)) + Math.floor(Math.random() * 180);
    return new Promise(resolve => setTimeout(resolve, delay));
}

async function executeTursoPipeline(statements, options = {}) {
    const config = sanitizeTursoSyncSettings(options.tursoSync || state.settings.tursoSync);
    if (!config.databaseUrl || !config.authToken) throw new Error("Turso URL або database auth token не задані");
    const requests = statements.map(statement => createTursoExecuteRequest(statement.sql, statement.args));
    requests.push({ type: "close" });
    const endpoint = `${config.databaseUrl}/v2/pipeline`;
    let lastError = null;
    for (let attempt = 1; attempt <= CONFIG.tursoHttpMaximumAttempts; attempt += 1) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), Math.max(1000, Number(options.timeoutMs) || CONFIG.tursoHttpTimeoutMs));
        try {
            const response = await fetch(endpoint, {
                method: "POST",
                headers: {
                    "Authorization": `Bearer ${config.authToken}`,
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({ requests }),
                signal: controller.signal,
                cache: "no-store"
            });
            const responseText = await response.text();
            let payload = null;
            try { payload = responseText ? JSON.parse(responseText) : {}; } catch (error) {}
            if (!response.ok) {
                const requestError = new Error(payload?.error || payload?.message || responseText || `Turso HTTP ${response.status}`);
                requestError.status = response.status;
                throw requestError;
            }
            return extractTursoPipelineResults(payload);
        } catch (error) {
            lastError = error?.name === "AbortError" ? new Error("Turso request timeout") : error;
            if (attempt >= CONFIG.tursoHttpMaximumAttempts || !shouldRetryTursoError(lastError)) throw lastError;
            await waitForTursoRetry(attempt);
        } finally {
            clearTimeout(timeout);
        }
    }
    throw lastError || new Error("Turso request failed");
}

function getTursoSchemaKey(config) {
    return `${config.databaseUrl}|${config.databaseName}|${TURSO_SIGNAL_SYNC.schemaVersion}`;
}

async function ensureTursoSignalSchema(tursoSync = state.settings.tursoSync) {
    const config = sanitizeTursoSyncSettings(tursoSync);
    const schemaKey = getTursoSchemaKey(config);
    if (state.tursoSync.schemaReadyKey === schemaKey) return true;
    await executeTursoPipeline([
        { sql: `CREATE TABLE IF NOT EXISTS ${TURSO_SIGNAL_SYNC.signalTableName} (id TEXT PRIMARY KEY, market TEXT NOT NULL, request_symbol TEXT NOT NULL, symbol TEXT NOT NULL DEFAULT '', side TEXT NOT NULL, minute_time INTEGER NOT NULL, trade_time INTEGER NOT NULL, price REAL NOT NULL, qty REAL NOT NULL DEFAULT 0, notional REAL NOT NULL, created_at INTEGER NOT NULL, source_updated_at INTEGER NOT NULL)` },
        { sql: `CREATE UNIQUE INDEX IF NOT EXISTS idx_alpha_signal_identity ON ${TURSO_SIGNAL_SYNC.signalTableName} (market, request_symbol, side, minute_time)` },
        { sql: `CREATE INDEX IF NOT EXISTS idx_alpha_signal_time_id ON ${TURSO_SIGNAL_SYNC.signalTableName} (minute_time, id)` },
        { sql: `CREATE INDEX IF NOT EXISTS idx_alpha_signal_market_symbol_time_id ON ${TURSO_SIGNAL_SYNC.signalTableName} (market, request_symbol, minute_time, id)` },
        { sql: `CREATE TABLE IF NOT EXISTS ${TURSO_SIGNAL_SYNC.metaTableName} (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL)` },
        { sql: `INSERT INTO ${TURSO_SIGNAL_SYNC.metaTableName} (key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`, args: ["schema_version", String(TURSO_SIGNAL_SYNC.schemaVersion), nowMs()] }
    ], { tursoSync: config });
    state.tursoSync.schemaReadyKey = schemaKey;
    return true;
}

function tursoRowsToObjects(result) {
    const columns = Array.isArray(result?.cols) ? result.cols.map(column => String(column?.name || "")) : [];
    const rows = Array.isArray(result?.rows) ? result.rows : [];
    return rows.map(row => {
        const value = {};
        for (let index = 0; index < columns.length; index += 1) {
            const cell = row[index];
            if (!cell || cell.type === "null") value[columns[index]] = null;
            else if (cell.type === "integer" || cell.type === "float") value[columns[index]] = Number(cell.value);
            else value[columns[index]] = cell.value;
        }
        return value;
    });
}

function clearTursoThresholdFallbackTimer() {
    const runtime = state.tursoThresholdFallback;
    if (!runtime?.timer) return;
    clearTimeout(runtime.timer);
    runtime.timer = null;
}

function getTursoThresholdFallbackNeeds(runtime = state.scalpingState?.advancedTradeThresholds) {
    const scalping = state.settings?.scalping || {};
    const enabled = scalping.enabled === true && scalping.advancedTradeThresholdsEnabled === true;
    return {
        large: enabled && Boolean(runtime) && runtime.bigTradesAutoReady !== true,
        significant: enabled && Boolean(runtime) && runtime.significantTradesAutoReady !== true
    };
}

function normalizeFreshTursoThreshold(value, updatedAtMs, referenceMs = nowMs()) {
    const numericValue = Number(value);
    const timestamp = Math.floor(Number(updatedAtMs) || 0);
    const current = Math.max(0, Math.floor(Number(referenceMs) || nowMs()));
    if (!Number.isFinite(numericValue) || numericValue < 100 || numericValue > 10000000 || timestamp <= 0) return null;
    const ageMs = current - timestamp;
    if (ageMs < 0 || ageMs > TURSO_THRESHOLD_FALLBACK.maximumAgeMs) return null;
    return { value: numericValue, updatedAtMs: timestamp, ageMs };
}

function isTursoThresholdSchemaUnavailableError(error) {
    const text = String(error?.message || error || "").toLowerCase();
    return (
        text.includes("no such table")
        && text.includes(TURSO_THRESHOLD_FALLBACK.tableName.toLowerCase())
    ) || text.includes("no such column");
}

function postTursoThresholdFallbackUpdate(identity, runtime, details = {}) {
    if (!identity || !runtime) return;
    post("TURSO_THRESHOLD_FALLBACK_UPDATE", {
        market: identity.market,
        requestSymbol: identity.requestSymbol,
        coin: identity.symbol,
        bigTradesThresholdUsd: runtime.bigTradesThresholdUsd,
        significantTradesThresholdUsd: runtime.significantTradesThresholdUsd,
        bigTradesAutoReady: runtime.bigTradesAutoReady === true,
        significantTradesAutoReady: runtime.significantTradesAutoReady === true,
        largeApplied: details.largeApplied === true,
        significantApplied: details.significantApplied === true,
        largeFresh: details.largeFresh === true,
        significantFresh: details.significantFresh === true,
        largeUpdatedAtMs: Math.max(0, Math.floor(Number(details.largeUpdatedAtMs) || 0)),
        significantUpdatedAtMs: Math.max(0, Math.floor(Number(details.significantUpdatedAtMs) || 0)),
        checkedAt: Math.max(0, Math.floor(Number(details.checkedAt) || nowMs())),
        reason: String(details.reason || "scheduled"),
        status: String(details.status || "checked")
    });
}

async function waitForCurrentTursoSignalOperation() {
    const inFlight = state.tursoSync?.inFlightPromise;
    if (!inFlight || typeof inFlight.then !== "function") return;
    try {
        await inFlight;
    } catch (error) {
        // Threshold fallback is independent from signal synchronization. A failed
        // signal sync must not suppress the read-only threshold SELECT.
    }
}

async function refreshTursoThresholdFallback(reason = "scheduled") {
    const token = state.selectedToken;
    const runtime = state.scalpingState?.advancedTradeThresholds;
    const identity = getBigBuySignalDatabaseIdentity(token);
    const needs = getTursoThresholdFallbackNeeds(runtime);
    if (!identity || !runtime || (!needs.large && !needs.significant)) {
        clearTursoThresholdFallbackTimer();
        return { skipped: true, reason: "auto_ready_or_no_token" };
    }
    if (!isTursoThresholdFallbackConfigured()) {
        clearTursoThresholdFallbackTimer();
        return { skipped: true, reason: "not_configured" };
    }

    const generation = state.tursoThresholdFallback.generation;
    // Scope in-flight de-duplication to the current selection/config generation.
    // A stale request from a previous token/market/credential generation must not
    // suppress the mandatory immediate read for the new lifecycle.
    const operationKey = `${generation}:${identity.key}`;
    const existing = state.tursoThresholdFallback.inFlightByKey.get(operationKey);
    if (existing) return existing;

    const selectedRuntime = runtime;
    const operation = (async () => {
        await waitForCurrentTursoSignalOperation();

        const columns = [];
        if (needs.large) columns.push("large_usd", "large_updated_at_ms");
        if (needs.significant) columns.push("significant_usd", "significant_updated_at_ms");
        if (columns.length === 0) return { skipped: true, reason: "became_ready" };

        let row = null;
        const checkedAt = nowMs();
        try {
            const results = await executeTursoPipeline([{
                sql: `SELECT ${columns.join(", ")} FROM ${TURSO_THRESHOLD_FALLBACK.tableName} WHERE coin = ? AND market = ? AND request_symbol = ? LIMIT 1`,
                args: [identity.symbol, identity.market, identity.requestSymbol]
            }]);
            row = tursoRowsToObjects(results[0])[0] || null;
            state.tursoThresholdFallback.lastError = "";
        } catch (error) {
            state.tursoThresholdFallback.lastCheckAt = checkedAt;
            state.tursoThresholdFallback.lastError = String(error?.message || error || "");
            if (!isTursoThresholdSchemaUnavailableError(error)) {
                post("LOG", { message: `Turso threshold fallback read failed (${identity.key}): ${state.tursoThresholdFallback.lastError}` });
            }
            row = null;
        }

        if (
            generation !== state.tursoThresholdFallback.generation
            || selectedRuntime !== state.scalpingState?.advancedTradeThresholds
            || getBigBuySignalDatabaseIdentity(state.selectedToken)?.key !== identity.key
        ) {
            return { skipped: true, reason: "stale_selection" };
        }

        const currentNeeds = getTursoThresholdFallbackNeeds(selectedRuntime);
        const largeFresh = currentNeeds.large
            ? normalizeFreshTursoThreshold(row?.large_usd, row?.large_updated_at_ms, checkedAt)
            : null;
        const significantFresh = currentNeeds.significant
            ? normalizeFreshTursoThreshold(row?.significant_usd, row?.significant_updated_at_ms, checkedAt)
            : null;

        let largeApplied = false;
        let significantApplied = false;

        if (currentNeeds.large) {
            if (largeFresh) {
                selectedRuntime.bigTradesThresholdUsd = largeFresh.value;
                selectedRuntime.tursoFallbackLargeActive = true;
                largeApplied = true;
            } else {
                selectedRuntime.tursoFallbackLargeActive = false;
                selectedRuntime.bigTradesThresholdUsd = clampNumber(
                    selectedRuntime.configuredFallbackBigTradesThresholdUsd,
                    100,
                    10000000,
                    ADVANCED_TRADE_THRESHOLD_DEFAULTS.bigTradesThresholdUsd
                );
            }
        }

        if (currentNeeds.significant) {
            if (significantFresh) {
                selectedRuntime.significantTradesThresholdUsd = Math.max(
                    selectedRuntime.bigTradesThresholdUsd,
                    significantFresh.value
                );
                selectedRuntime.tursoFallbackSignificantActive = true;
                selectedRuntime.significantTradesConfidence = 0;
                significantApplied = true;
            } else {
                selectedRuntime.tursoFallbackSignificantActive = false;
                selectedRuntime.significantTradesThresholdUsd = Math.max(
                    selectedRuntime.bigTradesThresholdUsd,
                    clampNumber(
                        selectedRuntime.configuredFallbackSignificantTradesThresholdUsd,
                        100,
                        10000000,
                        ADVANCED_TRADE_THRESHOLD_DEFAULTS.significantTradesThresholdUsd
                    )
                );
            }
        } else {
            selectedRuntime.significantTradesThresholdUsd = Math.max(
                selectedRuntime.bigTradesThresholdUsd,
                selectedRuntime.significantTradesThresholdUsd
            );
        }

        selectedRuntime.significantTradesReady =
            selectedRuntime.significantTradesThresholdUsd > selectedRuntime.bigTradesThresholdUsd;
        state.tursoThresholdFallback.lastCheckAt = checkedAt;
        if (largeApplied || significantApplied) state.tursoThresholdFallback.lastAppliedAt = checkedAt;

        postTursoThresholdFallbackUpdate(identity, selectedRuntime, {
            largeApplied,
            significantApplied,
            largeFresh: Boolean(largeFresh),
            significantFresh: Boolean(significantFresh),
            largeUpdatedAtMs: largeFresh?.updatedAtMs || 0,
            significantUpdatedAtMs: significantFresh?.updatedAtMs || 0,
            checkedAt,
            reason,
            status: row ? "row_checked" : "fallback_only"
        });
        postMetricsSnapshot("turso_threshold_fallback", true);

        return {
            skipped: false,
            largeApplied,
            significantApplied,
            largeFresh: Boolean(largeFresh),
            significantFresh: Boolean(significantFresh)
        };
    })();

    state.tursoThresholdFallback.inFlightByKey.set(operationKey, operation);
    try {
        return await operation;
    } finally {
        if (state.tursoThresholdFallback.inFlightByKey.get(operationKey) === operation) {
            state.tursoThresholdFallback.inFlightByKey.delete(operationKey);
        }
    }
}

function scheduleTursoThresholdFallbackRefresh(reason = "scheduled") {
    const runtime = state.scalpingState?.advancedTradeThresholds;
    const needs = getTursoThresholdFallbackNeeds(runtime);
    if (
        !state.selectedToken
        || !runtime
        || (!needs.large && !needs.significant)
        || !isTursoThresholdFallbackConfigured()
    ) {
        clearTursoThresholdFallbackTimer();
        return false;
    }
    if (state.tursoThresholdFallback.timer) return true;
    const generation = state.tursoThresholdFallback.generation;
    state.tursoThresholdFallback.timer = setTimeout(() => {
        state.tursoThresholdFallback.timer = null;
        if (generation !== state.tursoThresholdFallback.generation) return;
        void refreshTursoThresholdFallback(reason)
            .catch(error => {
                state.tursoThresholdFallback.lastError = String(error?.message || error || "");
            })
            .finally(() => {
                if (generation === state.tursoThresholdFallback.generation) {
                    scheduleTursoThresholdFallbackRefresh("scheduled");
                }
            });
    }, TURSO_THRESHOLD_FALLBACK.refreshIntervalMs);
    return true;
}

function reconcileTursoThresholdFallbackSchedule(reason = "reconcile", options = {}) {
    const runtime = state.scalpingState?.advancedTradeThresholds;
    const needs = getTursoThresholdFallbackNeeds(runtime);
    if (
        !state.selectedToken
        || !runtime
        || (!needs.large && !needs.significant)
        || !isTursoThresholdFallbackConfigured()
    ) {
        clearTursoThresholdFallbackTimer();
        return;
    }
    if (options.immediate === true) {
        void refreshTursoThresholdFallback(reason)
            .catch(error => {
                state.tursoThresholdFallback.lastError = String(error?.message || error || "");
            })
            .finally(() => scheduleTursoThresholdFallbackRefresh("scheduled"));
        return;
    }
    scheduleTursoThresholdFallbackRefresh("scheduled");
}

function startTursoThresholdFallbackLifecycle(reason = "token_selected") {
    clearTursoThresholdFallbackTimer();
    reconcileTursoThresholdFallbackSchedule(reason, { immediate: true });
}

function deactivateTursoThresholdFallback(reason = "disabled") {
    clearTursoThresholdFallbackTimer();
    // Requests already sent over HTTP cannot be unsent, but a disabled feature must
    // not retain de-duplication state or apply their eventual result. applySettings()
    // increments generation before calling this function, so stale completions fail
    // their generation check; clearing the registry also guarantees no future call
    // can reuse an operation that started while fallback was enabled.
    state.tursoThresholdFallback.inFlightByKey.clear();
    const runtime = state.scalpingState?.advancedTradeThresholds;
    const identity = getBigBuySignalDatabaseIdentity(state.selectedToken);
    if (!runtime || !identity) return;

    let changed = false;
    if (runtime.bigTradesAutoReady !== true && runtime.tursoFallbackLargeActive) {
        runtime.bigTradesThresholdUsd = clampNumber(
            runtime.configuredFallbackBigTradesThresholdUsd,
            100,
            10000000,
            ADVANCED_TRADE_THRESHOLD_DEFAULTS.bigTradesThresholdUsd
        );
        runtime.tursoFallbackLargeActive = false;
        changed = true;
    }
    if (runtime.significantTradesAutoReady !== true && runtime.tursoFallbackSignificantActive) {
        runtime.significantTradesThresholdUsd = Math.max(
            runtime.bigTradesThresholdUsd,
            clampNumber(
                runtime.configuredFallbackSignificantTradesThresholdUsd,
                100,
                10000000,
                ADVANCED_TRADE_THRESHOLD_DEFAULTS.significantTradesThresholdUsd
            )
        );
        runtime.tursoFallbackSignificantActive = false;
        runtime.significantTradesConfidence = 0;
        changed = true;
    }
    runtime.significantTradesThresholdUsd = Math.max(
        runtime.bigTradesThresholdUsd,
        runtime.significantTradesThresholdUsd
    );
    runtime.significantTradesReady = runtime.significantTradesThresholdUsd > runtime.bigTradesThresholdUsd;

    if (changed) {
        postTursoThresholdFallbackUpdate(identity, runtime, {
            reason,
            status: "disabled",
            checkedAt: nowMs()
        });
        postMetricsSnapshot("turso_threshold_fallback_disabled", true);
    }
}

function postTursoSyncStatus(status, message, extra = {}) {
    state.tursoSync.lastStatus = String(status || "idle");
    state.tursoSync.lastError = String(extra.error || "");
    post("TURSO_SYNC_STATUS", {
        status: state.tursoSync.lastStatus,
        message: String(message || ""),
        error: state.tursoSync.lastError,
        lastPullAt: Math.max(0, Number(extra.lastPullAt) || state.tursoSync.lastPullAt || 0),
        lastPushAt: Math.max(0, Number(extra.lastPushAt) || state.tursoSync.lastPushAt || 0),
        pulled: Math.max(0, Number(extra.pulled) || 0),
        pushed: Math.max(0, Number(extra.pushed) || 0)
    });
}

async function testTursoConnection(tursoSync) {
    const config = sanitizeTursoSyncSettings({ ...tursoSync, enabled: true });
    if (!config.databaseUrl) throw new Error("Некоректний Turso Database URL");
    if (!config.authToken) throw new Error("Відсутній database auth token");
    state.tursoSync.schemaReadyKey = "";
    await ensureTursoSignalSchema(config);
    const testKey = `connection_test_${workerBootId}`;
    const results = await executeTursoPipeline([
        { sql: `INSERT INTO ${TURSO_SIGNAL_SYNC.metaTableName} (key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`, args: [testKey, config.databaseName || "AlphaSpread", nowMs()] },
        { sql: `SELECT value FROM ${TURSO_SIGNAL_SYNC.metaTableName} WHERE key = ? LIMIT 1`, args: [testKey] },
        { sql: `DELETE FROM ${TURSO_SIGNAL_SYNC.metaTableName} WHERE key = ?`, args: [testKey] }
    ], { tursoSync: config });
    const verificationRows = tursoRowsToObjects(results[1]);
    if (verificationRows.length !== 1) throw new Error("Turso write verification failed");
    return { ok: true, databaseName: config.databaseName, databaseUrl: config.databaseUrl };
}

async function readTursoOutboxBatch(limit = TURSO_SIGNAL_SYNC.outboxBatchSize) {
    const database = await openBigBuySignalDatabase();
    if (!database) return [];
    const transaction = database.transaction(BIG_BUY_SIGNAL_DATABASE.outboxStoreName, "readonly");
    const completed = indexedDbTransactionToPromise(transaction);
    const index = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.outboxStoreName).index(BIG_BUY_SIGNAL_DATABASE.outboxQueuedAtIndex);
    const records = [];
    await new Promise((resolve, reject) => {
        const request = index.openCursor(null, "next");
        request.onerror = () => reject(request.error || new Error("IndexedDB outbox cursor failed"));
        request.onsuccess = () => {
            const cursor = request.result;
            if (!cursor || records.length >= limit) return resolve();
            records.push(cursor.value);
            cursor.continue();
        };
    });
    await completed;
    return records;
}

async function deleteTursoOutboxRecords(ids) {
    if (!Array.isArray(ids) || ids.length === 0) return 0;
    const database = await openBigBuySignalDatabase();
    if (!database) return 0;
    const transaction = database.transaction(BIG_BUY_SIGNAL_DATABASE.outboxStoreName, "readwrite");
    const completed = indexedDbTransactionToPromise(transaction);
    const store = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.outboxStoreName);
    for (const id of ids) store.delete(String(id));
    await completed;
    return ids.length;
}

function createTursoSignalUpsertStatement(record) {
    return {
        sql: `INSERT INTO ${TURSO_SIGNAL_SYNC.signalTableName} (id, market, request_symbol, symbol, side, minute_time, trade_time, price, qty, notional, created_at, source_updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(market, request_symbol, side, minute_time) DO UPDATE SET symbol = excluded.symbol, trade_time = excluded.trade_time, price = excluded.price, qty = excluded.qty, notional = excluded.notional, source_updated_at = excluded.source_updated_at WHERE excluded.trade_time < ${TURSO_SIGNAL_SYNC.signalTableName}.trade_time`,
        args: [record.id, record.market, record.requestSymbol, record.symbol || "", record.side, record.time, record.tradeTime, record.price, record.qty || 0, record.notional, record.createdAt || nowMs(), nowMs()]
    };
}

async function flushTursoSignalOutbox(reason = "timer") {
    if (!isTursoSyncConfigured()) return { pushed: 0, skipped: true };
    if (state.tursoSync.flushPromise) return state.tursoSync.flushPromise;
    const operation = (async () => {
        await ensureTursoSignalSchema();
        let pushed = 0;
        for (let batchIndex = 0; batchIndex < TURSO_SIGNAL_SYNC.maximumOutboxBatchesPerFlush; batchIndex += 1) {
            const records = await readTursoOutboxBatch();
            if (records.length === 0) break;
            await executeTursoPipeline(records.map(createTursoSignalUpsertStatement));
            await deleteTursoOutboxRecords(records.map(record => record.id));
            pushed += records.length;
            if (records.length < TURSO_SIGNAL_SYNC.outboxBatchSize) break;
        }
        if (pushed > 0) {
            state.tursoSync.lastPushAt = nowMs();
            postTursoSyncStatus("ready", `Turso: відправлено ${pushed} сигналів (${reason}).`, { pushed, lastPushAt: state.tursoSync.lastPushAt });
        }
        return { pushed };
    })();
    state.tursoSync.flushPromise = operation;
    try {
        return await operation;
    } finally {
        if (state.tursoSync.flushPromise === operation) state.tursoSync.flushPromise = null;
    }
}

function normalizeTursoSignalRow(row) {
    const market = sanitizeMarket(row.market, "");
    const requestSymbol = String(row.request_symbol || "").trim().toUpperCase();
    const side = row.side === "sell" ? "sell" : row.side === "buy" ? "buy" : "";
    const minuteTime = Math.floor(Number(row.minute_time) || 0);
    const tradeTime = Math.max(minuteTime, Math.floor(Number(row.trade_time) || minuteTime));
    const price = Number(row.price);
    const qty = Math.max(0, Number(row.qty) || 0);
    const notional = Number(row.notional);
    if (!MARKET_VALUES.includes(market) || !requestSymbol || !side || minuteTime <= 0 || !Number.isFinite(price) || price <= 0 || !Number.isFinite(notional) || notional <= 0) return null;
    const signalType = getMinuteTradeSignalType(side);
    return {
        id: `${market}:${requestSymbol}:${signalType}:${minuteTime}`,
        market,
        symbol: String(row.symbol || requestSymbol).trim().toUpperCase(),
        requestSymbol,
        time: minuteTime,
        tradeTime,
        price,
        qty,
        notional,
        side,
        signalType,
        label: signalType,
        createdAt: Math.max(0, Number(row.created_at) || nowMs())
    };
}

async function importTursoSignalsIntoLocalDatabase(records) {
    if (!Array.isArray(records) || records.length === 0) return 0;
    const database = await openBigBuySignalDatabase();
    if (!database) throw new Error("IndexedDB недоступна для імпорту Turso");
    return new Promise((resolve, reject) => {
        const transaction = database.transaction(BIG_BUY_SIGNAL_DATABASE.storeName, "readwrite");
        const store = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.storeName);
        let imported = 0;
        let settled = false;
        const finish = (error = null) => {
            if (settled) return;
            settled = true;
            if (error) reject(error);
            else resolve(imported);
        };
        transaction.oncomplete = () => finish();
        transaction.onabort = () => finish(transaction.error || new Error("IndexedDB Turso import aborted"));
        transaction.onerror = () => {};
        for (const record of records) {
            const getRequest = store.get(record.id);
            getRequest.onsuccess = () => {
                const existing = getRequest.result;
                const existingTradeTime = Math.max(0, Number(existing?.tradeTime) || Number.MAX_SAFE_INTEGER);
                if (!existing || record.tradeTime < existingTradeTime) {
                    store.put(record);
                    imported += 1;
                }
            };
            getRequest.onerror = () => {
                try { transaction.abort(); } catch (error) {}
            };
        }
    });
}

async function pullTursoSignals(startTimeSeconds, endTimeSeconds) {
    if (!isTursoSyncConfigured()) return { pulled: 0, imported: 0 };
    await ensureTursoSignalSchema();
    const startTime = Math.max(1, Math.floor(Number(startTimeSeconds) || 1));
    const endTime = Math.max(startTime, Math.floor(Number(endTimeSeconds) || Math.floor(nowMs() / 1000)));
    let cursorTime = startTime;
    let cursorId = "";
    let pulled = 0;
    let imported = 0;
    while (true) {
        const [result] = await executeTursoPipeline([{
            sql: `SELECT id, market, request_symbol, symbol, side, minute_time, trade_time, price, qty, notional, created_at FROM ${TURSO_SIGNAL_SYNC.signalTableName} WHERE minute_time >= ? AND minute_time <= ? AND (minute_time > ? OR (minute_time = ? AND id > ?)) ORDER BY minute_time ASC, id ASC LIMIT ?`,
            args: [startTime, endTime, cursorTime, cursorTime, cursorId, TURSO_SIGNAL_SYNC.pullPageSize]
        }]);
        const rows = tursoRowsToObjects(result);
        const normalized = rows.map(normalizeTursoSignalRow).filter(Boolean);
        pulled += normalized.length;
        imported += await importTursoSignalsIntoLocalDatabase(normalized);
        if (pulled > CONFIG.tursoMaximumPullRows) throw new Error("Turso sync row limit exceeded; звузьте період синхронізації");
        if (rows.length < TURSO_SIGNAL_SYNC.pullPageSize) break;
        const last = rows[rows.length - 1];
        cursorTime = Math.max(cursorTime, Math.floor(Number(last.minute_time) || cursorTime));
        cursorId = String(last.id || "");
    }
    state.tursoSync.lastPullAt = endTime * 1000;
    if (imported > 0 && state.selectedToken) {
        await loadAndPostBigBuyMinuteSignalHistory(state.selectedToken, "turso_sync_import");
    }
    return { pulled, imported, lastPullAt: state.tursoSync.lastPullAt };
}

async function pullTursoSignalsForToken(identity, startTimeSeconds, endTimeSeconds) {
    if (!identity || !isTursoSyncConfigured()) return { pulled: 0, imported: 0 };
    await ensureTursoSignalSchema();
    const startTime = Math.max(1, Math.floor(Number(startTimeSeconds) || 1));
    const endTime = Math.max(startTime, Math.floor(Number(endTimeSeconds) || Math.floor(nowMs() / 1000)));
    let cursorTime = startTime;
    let cursorId = "";
    let pulled = 0;
    let imported = 0;
    let scanned = 0;

    while (true) {
        const [result] = await executeTursoPipeline([{
            sql: `SELECT id, market, request_symbol, symbol, side, minute_time, trade_time, price, qty, notional, created_at FROM ${TURSO_SIGNAL_SYNC.signalTableName} WHERE market = ? AND request_symbol = ? AND minute_time >= ? AND minute_time <= ? AND (minute_time > ? OR (minute_time = ? AND id > ?)) ORDER BY minute_time ASC, id ASC LIMIT ?`,
            args: [identity.market, identity.requestSymbol, startTime, endTime, cursorTime, cursorTime, cursorId, TURSO_SIGNAL_SYNC.pullPageSize]
        }]);
        const rows = tursoRowsToObjects(result);
        scanned += rows.length;
        const normalized = rows.map(normalizeTursoSignalRow).filter(record => (
            record && record.market === identity.market && record.requestSymbol === identity.requestSymbol
        ));
        pulled += normalized.length;
        if (normalized.length > 0) imported += await importTursoSignalsIntoLocalDatabase(normalized);
        if (scanned > CONFIG.tursoMaximumPullRows) {
            throw new Error(`Turso token sync row limit exceeded for ${identity.key}; звузьте період синхронізації`);
        }
        if (rows.length < TURSO_SIGNAL_SYNC.pullPageSize) break;
        const last = rows[rows.length - 1];
        cursorTime = Math.max(cursorTime, Math.floor(Number(last.minute_time) || cursorTime));
        cursorId = String(last.id || "");
    }

    return { pulled, imported, startTime, endTime };
}

async function waitForMinuteTradeSignalHistoryLoad(identity) {
    if (!identity) return;
    const prefix = `${identity.key}:`;
    const pending = [];
    for (const [loadKey, promise] of state.bigBuySignalDatabase.inFlightLoads.entries()) {
        if (String(loadKey).startsWith(prefix) && promise) pending.push(promise);
    }
    if (pending.length > 0) await Promise.allSettled(pending);
}

async function synchronizeTursoTokenSignals(payload = {}) {
    if (payload.settings) applySettings(payload.settings);
    if (!isTursoSyncConfigured()) return { skipped: true, pulled: 0, imported: 0 };
    const identity = getBigBuySignalDatabaseIdentity(payload.token);
    if (!identity) return { skipped: true, pulled: 0, imported: 0, reason: "invalid_token" };
    const rawStartTime = Math.floor(Number(payload.startTimeSeconds) || 0);
    if (rawStartTime <= 0) return { skipped: true, pulled: 0, imported: 0, reason: "missing_start_time" };
    const startTime = Math.max(1, rawStartTime);
    const endTime = Math.max(startTime, Math.floor(Number(payload.endTimeSeconds) || Math.floor(nowMs() / 1000)));
    const operationKey = identity.key;
    const existingEntry = state.tursoSync.tokenPullPromises.get(operationKey);
    if (existingEntry
        && existingEntry.startTime <= startTime
        && existingEntry.endTime >= endTime) {
        return existingEntry.promise;
    }

    const predecessor = existingEntry?.promise || Promise.resolve(null);
    const operation = predecessor
        .catch(() => null)
        .then(async previousResult => {
            const pullResult = await pullTursoSignalsForToken(identity, startTime, endTime);
            const previousImported = Math.max(0, Number(previousResult?.selectionImported ?? previousResult?.imported) || 0);
            return {
                ...pullResult,
                selectionImported: previousImported + Math.max(0, Number(pullResult.imported) || 0)
            };
        });
    const entry = { promise: operation, startTime, endTime };
    state.tursoSync.tokenPullPromises.set(operationKey, entry);
    try {
        return await operation;
    } finally {
        if (state.tursoSync.tokenPullPromises.get(operationKey) === entry) {
            state.tursoSync.tokenPullPromises.delete(operationKey);
        }
    }
}

async function reloadMinuteTradeSignalHistory(payload = {}) {
    if (payload.settings) applySettings(payload.settings);
    const token = payload.token;
    const identity = getBigBuySignalDatabaseIdentity(token);
    if (!identity) return { skipped: true, reloaded: false, reason: "invalid_token" };
    if (!isMinuteTradeSignalHistoryEnabled()) return { skipped: true, reloaded: false, reason: "history_disabled" };
    await waitForMinuteTradeSignalHistoryLoad(identity);
    const history = await loadAndPostBigBuyMinuteSignalHistory(token, String(payload.reason || "manual_reload"));
    return {
        reloaded: true,
        buyCount: Array.isArray(history?.buySignals) ? history.buySignals.length : 0,
        sellCount: Array.isArray(history?.sellSignals) ? history.sellSignals.length : 0
    };
}

async function synchronizeTursoSignals(payload = {}) {
    if (state.tursoSync.inFlightPromise) return state.tursoSync.inFlightPromise;
    if (payload.settings) applySettings(payload.settings);
    if (!isTursoSyncConfigured()) return { skipped: true, pulled: 0, pushed: 0 };
    const reason = String(payload.reason || "manual");
    const operation = (async () => {
        postTursoSyncStatus("syncing", `Turso: синхронізація (${reason})…`);
        try {
            const pushResult = await flushTursoSignalOutbox(reason);
            const pullResult = await pullTursoSignals(payload.startTimeSeconds, payload.endTimeSeconds);
            const result = { pushed: pushResult.pushed || 0, pulled: pullResult.pulled || 0, imported: pullResult.imported || 0, lastPullAt: pullResult.lastPullAt || 0, lastPushAt: state.tursoSync.lastPushAt || 0 };
            postTursoSyncStatus("ready", `Turso: синхронізацію завершено (${reason}).`, result);
            return result;
        } catch (error) {
            postTursoSyncStatus("error", `Turso: ${error?.message || "помилка синхронізації"}`, { error: error?.message || String(error) });
            throw error;
        }
    })();
    state.tursoSync.inFlightPromise = operation;
    try {
        return await operation;
    } finally {
        if (state.tursoSync.inFlightPromise === operation) state.tursoSync.inFlightPromise = null;
    }
}

function normalizeDatabaseMinuteTradeSignal(value, identity, expectedSignalType) {
    if (!value || value.signalType !== expectedSignalType) return null;
    const side = expectedSignalType === BIG_BUY_SIGNAL_DATABASE.sellSignalType ? "sell" : "buy";
    const time = Math.floor(Number(value.time) || 0);
    const price = Number(value.price);
    const notional = Number(value.notional);
    if (time <= 0 || !Number.isFinite(price) || price <= 0 || !Number.isFinite(notional) || notional <= 0) return null;
    return {
        id: String(value.id || `${identity.requestSymbol}_${side}_${time}`),
        time,
        tradeTime: Math.floor(Number(value.tradeTime) || time),
        price,
        qty: Number(value.qty) || 0,
        notional,
        side,
        signalType: expectedSignalType
    };
}

async function readMinuteTradeSignalsFromDatabaseRange(identity, startTimeSeconds, endTimeSeconds, side = "buy", options = {}) {
    if (!identity) return [];
    const startTime = Math.max(0, Math.floor(Number(startTimeSeconds) || 0));
    const endTime = Math.max(startTime, Math.floor(Number(endTimeSeconds) || 0));
    if (startTime <= 0 || endTime < startTime) return [];
    const expectedSignalType = getMinuteTradeSignalType(side);
    const database = await openBigBuySignalDatabase();
    if (!database) return [];

    try {
        const transaction = database.transaction(BIG_BUY_SIGNAL_DATABASE.storeName, "readonly");
        const completed = indexedDbTransactionToPromise(transaction);
        const index = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.storeName)
            .index(BIG_BUY_SIGNAL_DATABASE.marketSymbolTimeIndex);
        const keyRange = IDBKeyRange.bound(
            [identity.market, identity.requestSymbol, expectedSignalType, startTime],
            [identity.market, identity.requestSymbol, expectedSignalType, endTime]
        );
        const signals = [];
        await new Promise((resolve, reject) => {
            const request = index.openCursor(keyRange, "next");
            request.onerror = () => reject(request.error || new Error("IndexedDB cursor failed"));
            request.onsuccess = () => {
                const cursor = request.result;
                if (!cursor || signals.length >= BIG_BUY_SIGNAL_DATABASE.maximumRecordsPerLoad) {
                    resolve();
                    return;
                }
                const normalized = normalizeDatabaseMinuteTradeSignal(cursor.value, identity, expectedSignalType);
                if (normalized) signals.push(normalized);
                cursor.continue();
            };
        });
        await completed;
        return signals;
    } catch (error) {
        reportBigBuySignalDatabaseError(`load_${expectedSignalType}_signals_range`, error);
        if (options.throwOnError === true) throw error;
        return [];
    }
}

async function readBigBuyMinuteSignalsFromDatabaseRange(identity, startTimeSeconds, endTimeSeconds) {
    return readMinuteTradeSignalsFromDatabaseRange(identity, startTimeSeconds, endTimeSeconds, "buy");
}

async function readBigSellMinuteSignalsFromDatabaseRange(identity, startTimeSeconds, endTimeSeconds) {
    return readMinuteTradeSignalsFromDatabaseRange(identity, startTimeSeconds, endTimeSeconds, "sell");
}

const BIG_BUY_FIVE_MINUTE_DATABASE_BUCKET_SECONDS = BIG_BUY_SIGNAL_DATABASE.timeframeSeconds * 5;

async function readBigBuyFiveMinuteSignalSourcesFromDatabaseRange(identity, startTimeSeconds, endTimeSeconds, options = {}) {
    if (!identity) return [];
    const startTime = Math.max(1, Math.floor(Number(startTimeSeconds) || 0));
    const endTime = Math.max(startTime, Math.floor(Number(endTimeSeconds) || startTime));
    if (endTime < startTime) return [];

    const expectedSignalType = BIG_BUY_SIGNAL_DATABASE.buySignalType;
    const database = await openBigBuySignalDatabase();
    if (!database) return [];

    try {
        const transaction = database.transaction(BIG_BUY_SIGNAL_DATABASE.storeName, "readonly");
        const completed = indexedDbTransactionToPromise(transaction);
        const index = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.storeName)
            .index(BIG_BUY_SIGNAL_DATABASE.marketSymbolTimeIndex);
        const keyRange = IDBKeyRange.bound(
            [identity.market, identity.requestSymbol, expectedSignalType, startTime],
            [identity.market, identity.requestSymbol, expectedSignalType, endTime]
        );

        let rawRecords;
        if (typeof index.getAll === "function") {
            // V165 fast path: one native IndexedDB bulk read for the whole 5m chart
            // time range. No cursor callback/continue round trip per stored signal.
            rawRecords = await indexedDbRequestToPromise(index.getAll(keyRange));
        } else {
            // Compatibility fallback only. Modern browsers use getAll above.
            rawRecords = [];
            await new Promise((resolve, reject) => {
                const request = index.openCursor(keyRange, "next");
                request.onerror = () => reject(request.error || new Error("IndexedDB 5m fallback cursor failed"));
                request.onsuccess = () => {
                    const cursor = request.result;
                    if (!cursor) {
                        resolve();
                        return;
                    }
                    rawRecords.push(cursor.value);
                    cursor.continue();
                };
            });
        }
        await completed;

        const scalpingSettings = state.settings?.scalping || {};
        if (scalpingSettings.enabled !== true || scalpingSettings.bigBuyFiveMinuteMarkersEnabled !== true) {
            return [];
        }

        // The fixed market/symbol/type composite-index range is returned in ascending
        // time order. A single linear pass is therefore enough: keep only the first
        // valid signal encountered for every 300-second bucket.
        const signals = [];
        let lastBucketStart = -1;
        for (let recordIndex = 0; recordIndex < rawRecords.length; recordIndex += 1) {
            // If the option is switched off while a large bulk result is being processed,
            // stop immediately. Checking once per 256 records keeps the hot loop cheap.
            if ((recordIndex & 255) === 0) {
                const currentScalpingSettings = state.settings?.scalping || {};
                if (currentScalpingSettings.enabled !== true
                    || currentScalpingSettings.bigBuyFiveMinuteMarkersEnabled !== true) {
                    return [];
                }
            }

            const normalized = normalizeDatabaseMinuteTradeSignal(rawRecords[recordIndex], identity, expectedSignalType);
            if (!normalized) continue;
            const bucketStart = Math.floor(normalized.time / BIG_BUY_FIVE_MINUTE_DATABASE_BUCKET_SECONDS)
                * BIG_BUY_FIVE_MINUTE_DATABASE_BUCKET_SECONDS;
            if (bucketStart === lastBucketStart) continue;
            signals.push(normalized);
            lastBucketStart = bucketStart;
        }
        return signals;
    } catch (error) {
        reportBigBuySignalDatabaseError("load_big_buy_5m_signal_sources_range", error);
        if (options.throwOnError === true) throw error;
        return [];
    }
}

async function loadBigBuyFiveMinuteSignalSourceRange(payload = {}) {
    if (payload.settings) applySettings(payload.settings);
    const scalpingSettings = state.settings?.scalping || {};
    if (scalpingSettings.enabled !== true || scalpingSettings.bigBuyFiveMinuteMarkersEnabled !== true) {
        return { skipped: true, reason: "five_minute_signals_disabled", signals: [] };
    }

    const identity = getBigBuySignalDatabaseIdentity(payload.token);
    if (!identity) return { skipped: true, reason: "invalid_token", signals: [] };
    const startTime = Math.max(1, Math.floor(Number(payload.startTime) || 0));
    const lastFiveMinuteCandleTime = Math.max(startTime, Math.floor(Number(payload.endTime) || startTime));
    const sourceEndTime = lastFiveMinuteCandleTime + BIG_BUY_SIGNAL_DATABASE.timeframeSeconds * 4;
    const signals = await readBigBuyFiveMinuteSignalSourcesFromDatabaseRange(identity, startTime, sourceEndTime, { throwOnError: true });
    return {
        skipped: false,
        market: identity.market,
        requestSymbol: identity.requestSymbol,
        startTime,
        endTime: sourceEndTime,
        signals
    };
}

async function readMinuteTradeSignalsFromDatabase(identity, referenceMs = nowMs(), side = "buy", options = {}) {
    const { minimumTime, currentMinuteTime } = getBigBuySignalDatabaseRange(referenceMs);
    return readMinuteTradeSignalsFromDatabaseRange(identity, minimumTime, currentMinuteTime, side, options);
}

async function loadAndPostBigBuyMinuteSignalHistory(token = state.selectedToken, reason = "token_selected") {
    const shouldLoadMinuteTradeSignalHistory = isMinuteTradeSignalHistoryEnabled();
    if (!shouldLoadMinuteTradeSignalHistory) return { buySignals: [], sellSignals: [] };
    const identity = getBigBuySignalDatabaseIdentity(token);
    if (!identity) return { buySignals: [], sellSignals: [] };

    const runtime = state.bigBuySignalDatabase;
    const shouldLoadBuys = shouldLoadMinuteTradeSignalHistory;
    const shouldLoadSells = shouldLoadMinuteTradeSignalHistory;
    const loadKey = `${identity.key}:${shouldLoadBuys ? 1 : 0}:${shouldLoadSells ? 1 : 0}`;
    const existingLoad = runtime.inFlightLoads.get(loadKey);
    if (existingLoad) return existingLoad;

    const loadSerial = ++runtime.loadSerial;
    runtime.latestLoadSerialByKey.set(identity.key, loadSerial);
    const loadPromise = (async () => {
        const [buySignals, sellSignals] = await Promise.all([
            shouldLoadBuys ? readMinuteTradeSignalsFromDatabase(identity, nowMs(), "buy") : Promise.resolve([]),
            shouldLoadSells ? readMinuteTradeSignalsFromDatabase(identity, nowMs(), "sell") : Promise.resolve([])
        ]);
        if (runtime.latestLoadSerialByKey.get(identity.key) !== loadSerial) return { buySignals: [], sellSignals: [] };

        const selectedIdentity = getBigBuySignalDatabaseIdentity(state.selectedToken);
        if (selectedIdentity?.key === identity.key && state.scalpingState) {
            if (buySignals.length > 0) {
                const latestBuyMinute = Math.max(0, Number(buySignals[buySignals.length - 1]?.time) || 0);
                state.scalpingState.lastBigBuyMinuteSignalMinute = Math.max(
                    Math.max(0, Number(state.scalpingState.lastBigBuyMinuteSignalMinute) || 0),
                    latestBuyMinute
                );
            }
            if (sellSignals.length > 0) {
                const latestSellMinute = Math.max(0, Number(sellSignals[sellSignals.length - 1]?.time) || 0);
                state.scalpingState.lastBigSellMinuteSignalMinute = Math.max(
                    Math.max(0, Number(state.scalpingState.lastBigSellMinuteSignalMinute) || 0),
                    latestSellMinute
                );
            }
        }

        const minimumTime = getBigBuySignalDatabaseRange().minimumTime;
        if (shouldLoadBuys) {
            post("BIG_BUY_MINUTE_SIGNAL_HISTORY", {
                market: identity.market,
                requestSymbol: identity.requestSymbol,
                reason,
                minimumTime,
                signals: buySignals
            });
        }
        if (shouldLoadSells) {
            post("BIG_SELL_MINUTE_SIGNAL_HISTORY", {
                market: identity.market,
                requestSymbol: identity.requestSymbol,
                reason,
                minimumTime,
                signals: sellSignals
            });
        }
        return { buySignals, sellSignals };
    })();

    runtime.inFlightLoads.set(loadKey, loadPromise);
    try {
        return await loadPromise;
    } finally {
        if (runtime.inFlightLoads.get(loadKey) === loadPromise) {
            runtime.inFlightLoads.delete(loadKey);
        }
    }
}

function getMinuteTradeSignalDatabaseIdentityRange(identity, signalType) {
    return IDBKeyRange.bound(
        [identity.market, identity.requestSymbol, signalType, 0],
        [identity.market, identity.requestSymbol, signalType, Number.MAX_SAFE_INTEGER]
    );
}

function deleteMinuteTradeSignalCursorRecords(index, keyRange, shouldDelete) {
    return new Promise((resolve, reject) => {
        const result = { scanned: 0, deleted: 0, retained: 0 };
        const request = index.openCursor(keyRange, "next");
        request.onerror = () => reject(request.error || new Error("IndexedDB maintenance cursor failed"));
        request.onsuccess = () => {
            const cursor = request.result;
            if (!cursor) {
                resolve(result);
                return;
            }

            result.scanned += 1;
            let deleteRecord = false;
            try {
                deleteRecord = shouldDelete(cursor.value, result) === true;
            } catch (error) {
                reject(error);
                return;
            }

            if (!deleteRecord) {
                result.retained += 1;
                cursor.continue();
                return;
            }

            const deleteRequest = cursor.delete();
            deleteRequest.onerror = () => reject(deleteRequest.error || new Error("IndexedDB maintenance delete failed"));
            deleteRequest.onsuccess = () => {
                result.deleted += 1;
                cursor.continue();
            };
        };
    });
}

async function clearMinuteTradeSignalsForDatabaseIdentity(identity) {
    const database = await openBigBuySignalDatabase();
    if (!database) throw new Error("IndexedDB недоступна");

    const transaction = database.transaction(BIG_BUY_SIGNAL_DATABASE.storeName, "readwrite");
    const completed = indexedDbTransactionToPromise(transaction);
    const index = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.storeName)
        .index(BIG_BUY_SIGNAL_DATABASE.marketSymbolTimeIndex);
    const buyPromise = deleteMinuteTradeSignalCursorRecords(
        index,
        getMinuteTradeSignalDatabaseIdentityRange(identity, BIG_BUY_SIGNAL_DATABASE.buySignalType),
        () => true
    );
    const sellPromise = deleteMinuteTradeSignalCursorRecords(
        index,
        getMinuteTradeSignalDatabaseIdentityRange(identity, BIG_BUY_SIGNAL_DATABASE.sellSignalType),
        () => true
    );
    const [buyResult, sellResult] = await Promise.all([buyPromise, sellPromise, completed]);
    return {
        deletedBuy: buyResult.deleted,
        deletedSell: sellResult.deleted,
        retainedBuy: 0,
        retainedSell: 0,
        scannedBuy: buyResult.scanned,
        scannedSell: sellResult.scanned
    };
}

async function thinMinuteTradeSignalsForDatabaseIdentity(identity) {
    const database = await openBigBuySignalDatabase();
    if (!database) throw new Error("IndexedDB недоступна");

    const transaction = database.transaction(BIG_BUY_SIGNAL_DATABASE.storeName, "readwrite");
    const completed = indexedDbTransactionToPromise(transaction);
    const index = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.storeName)
        .index(BIG_BUY_SIGNAL_DATABASE.marketSymbolTimeIndex);

    const createThinningPredicate = () => {
        let lastRetainedMinute = Number.NEGATIVE_INFINITY;
        return value => {
            const minuteTime = Math.floor(Number(value?.time) || 0);
            if (minuteTime <= 0) return true;
            if (minuteTime - lastRetainedMinute < MINUTE_TRADE_SIGNAL_MINIMUM_SPACING_SECONDS) return true;
            lastRetainedMinute = minuteTime;
            return false;
        };
    };

    const buyPromise = deleteMinuteTradeSignalCursorRecords(
        index,
        getMinuteTradeSignalDatabaseIdentityRange(identity, BIG_BUY_SIGNAL_DATABASE.buySignalType),
        createThinningPredicate()
    );
    const sellPromise = deleteMinuteTradeSignalCursorRecords(
        index,
        getMinuteTradeSignalDatabaseIdentityRange(identity, BIG_BUY_SIGNAL_DATABASE.sellSignalType),
        createThinningPredicate()
    );
    const [buyResult, sellResult] = await Promise.all([buyPromise, sellPromise, completed]);
    return {
        deletedBuy: buyResult.deleted,
        deletedSell: sellResult.deleted,
        retainedBuy: buyResult.retained,
        retainedSell: sellResult.retained,
        scannedBuy: buyResult.scanned,
        scannedSell: sellResult.scanned
    };
}

function queueMinuteTradeSignalDuringDatabaseMaintenance(identity, signal, side) {
    const runtime = state.bigBuySignalDatabase;
    let pendingSignals = runtime.pendingSignalsByKey.get(identity.key);
    if (!pendingSignals) {
        pendingSignals = new Map();
        runtime.pendingSignalsByKey.set(identity.key, pendingSignals);
    }
    const normalizedSide = side === "sell" ? "sell" : "buy";
    const minuteTime = Math.floor(Number(signal?.time) || 0);
    const pendingKey = `${getMinuteTradeSignalType(normalizedSide)}:${minuteTime}`;
    if (pendingSignals.has(pendingKey)) return false;
    pendingSignals.set(pendingKey, {
        identity: { ...identity },
        signal: { ...signal },
        side: normalizedSide
    });
    return true;
}

function requeueMinuteTradeSignalMaintenanceEntries(identity, entries, startIndex = 0) {
    const runtime = state.bigBuySignalDatabase;
    let pendingSignals = runtime.pendingSignalsByKey.get(identity.key);
    if (!pendingSignals) {
        pendingSignals = new Map();
        runtime.pendingSignalsByKey.set(identity.key, pendingSignals);
    }
    for (let index = Math.max(0, startIndex); index < entries.length; index += 1) {
        const entry = entries[index];
        const minuteTime = Math.floor(Number(entry.signal?.time) || 0);
        const pendingKey = `${getMinuteTradeSignalType(entry.side)}:${minuteTime}`;
        if (!pendingSignals.has(pendingKey)) pendingSignals.set(pendingKey, entry);
    }
}

async function flushPendingMinuteTradeSignalsForDatabaseIdentity(identity) {
    const runtime = state.bigBuySignalDatabase;
    const pendingSignals = runtime.pendingSignalsByKey.get(identity.key);
    if (!pendingSignals || pendingSignals.size === 0) {
        runtime.pendingSignalsByKey.delete(identity.key);
        return { inserted: 0, duplicate: 0 };
    }

    runtime.pendingSignalsByKey.delete(identity.key);
    const entries = [...pendingSignals.values()].sort((left, right) => {
        const timeDifference = Number(left.signal?.time) - Number(right.signal?.time);
        return timeDifference || String(left.side).localeCompare(String(right.side));
    });
    let inserted = 0;
    let duplicate = 0;
    for (let index = 0; index < entries.length; index += 1) {
        const entry = entries[index];
        const result = await persistBigBuyMinuteSignalToDatabase(entry.identity, entry.signal, entry.side);
        if (result.inserted) {
            inserted += 1;
            continue;
        }
        if (result.duplicate) {
            duplicate += 1;
            continue;
        }

        requeueMinuteTradeSignalMaintenanceEntries(identity, entries, index);
        if (result.available === false) {
            throw new Error("IndexedDB недоступна під час збереження нових маркерів");
        }
        throw new Error("Не вдалося зберегти новий маркер під час операції з IndexedDB");
    }
    return { inserted, duplicate };
}

function postAuthoritativeMinuteTradeSignalHistory(identity, buySignals, sellSignals, reason) {
    const minimumTime = getBigBuySignalDatabaseRange().minimumTime;
    post("BIG_BUY_MINUTE_SIGNAL_HISTORY", {
        market: identity.market,
        requestSymbol: identity.requestSymbol,
        reason,
        minimumTime,
        replaceExisting: true,
        signals: buySignals
    });
    post("BIG_SELL_MINUTE_SIGNAL_HISTORY", {
        market: identity.market,
        requestSymbol: identity.requestSymbol,
        reason,
        minimumTime,
        replaceExisting: true,
        signals: sellSignals
    });
}

function resetSelectedMinuteTradeSignalRuntimeForClear(identity) {
    const selectedIdentity = getBigBuySignalDatabaseIdentity(state.selectedToken);
    if (selectedIdentity?.key !== identity.key || !state.scalpingState) return;
    state.scalpingState.lastBigBuyMinuteSignalMinute = 0;
    state.scalpingState.lastBigSellMinuteSignalMinute = 0;
}

function synchronizeSelectedMinuteTradeSignalRuntimeFromHistory(identity, buySignals, sellSignals) {
    const selectedIdentity = getBigBuySignalDatabaseIdentity(state.selectedToken);
    if (selectedIdentity?.key !== identity.key || !state.scalpingState) return;
    state.scalpingState.lastBigBuyMinuteSignalMinute = buySignals.length > 0
        ? Math.max(0, Number(buySignals[buySignals.length - 1]?.time) || 0)
        : 0;
    state.scalpingState.lastBigSellMinuteSignalMinute = sellSignals.length > 0
        ? Math.max(0, Number(sellSignals[sellSignals.length - 1]?.time) || 0)
        : 0;
}

async function recoverMinuteTradeSignalHistoryAfterMaintenanceFailure(identity, reason) {
    try {
        const [buySignals, sellSignals] = await Promise.all([
            readMinuteTradeSignalsFromDatabase(identity, nowMs(), "buy", { throwOnError: true }),
            readMinuteTradeSignalsFromDatabase(identity, nowMs(), "sell", { throwOnError: true })
        ]);
        synchronizeSelectedMinuteTradeSignalRuntimeFromHistory(identity, buySignals, sellSignals);
        postAuthoritativeMinuteTradeSignalHistory(identity, buySignals, sellSignals, reason);
        return true;
    } catch (error) {
        reportBigBuySignalDatabaseError("recover_minute_trade_signal_history", error);
        return false;
    }
}

function releasePendingMinuteTradeSignalsWithoutDatabase(identity) {
    const runtime = state.bigBuySignalDatabase;
    const pendingSignals = runtime.pendingSignalsByKey.get(identity.key);
    runtime.pendingSignalsByKey.delete(identity.key);
    if (!pendingSignals || pendingSignals.size === 0) return 0;

    const entries = [...pendingSignals.values()].sort((left, right) => {
        const timeDifference = Number(left.signal?.time) - Number(right.signal?.time);
        return timeDifference || String(left.side).localeCompare(String(right.side));
    });
    for (const entry of entries) {
        post(entry.side === "sell" ? "BIG_SELL_MINUTE_SIGNAL" : "BIG_BUY_MINUTE_SIGNAL", {
            market: entry.identity.market,
            requestSymbol: entry.identity.requestSymbol,
            signal: entry.signal
        });
    }
    return entries.length;
}

async function synchronizeMinuteTradeSignalDatabaseAfterMaintenance(identity, operation, operationResult, reason) {
    const runtime = state.bigBuySignalDatabase;
    let flushedInserted = 0;
    let flushedDuplicate = 0;

    while (true) {
        const flushResult = await flushPendingMinuteTradeSignalsForDatabaseIdentity(identity);
        flushedInserted += flushResult.inserted;
        flushedDuplicate += flushResult.duplicate;

        if (operation === MINUTE_TRADE_SIGNAL_DATABASE_OPERATION.THIN && flushResult.inserted > 0) {
            const followUpThinning = await thinMinuteTradeSignalsForDatabaseIdentity(identity);
            operationResult.deletedBuy += followUpThinning.deletedBuy;
            operationResult.deletedSell += followUpThinning.deletedSell;
            operationResult.retainedBuy = followUpThinning.retainedBuy;
            operationResult.retainedSell = followUpThinning.retainedSell;
            operationResult.scannedBuy = followUpThinning.scannedBuy;
            operationResult.scannedSell = followUpThinning.scannedSell;
        }

        const [buySignals, sellSignals] = await Promise.all([
            readMinuteTradeSignalsFromDatabase(identity, nowMs(), "buy", { throwOnError: true }),
            readMinuteTradeSignalsFromDatabase(identity, nowMs(), "sell", { throwOnError: true })
        ]);
        const pendingSignals = runtime.pendingSignalsByKey.get(identity.key);
        if (pendingSignals && pendingSignals.size > 0) continue;

        synchronizeSelectedMinuteTradeSignalRuntimeFromHistory(identity, buySignals, sellSignals);
        postAuthoritativeMinuteTradeSignalHistory(identity, buySignals, sellSignals, reason);
        runtime.maintenanceKeys.delete(identity.key);
        return { buySignals, sellSignals, flushedInserted, flushedDuplicate };
    }
}

async function maintainMinuteTradeSignalDatabase(payload) {
    const operation = payload?.operation === MINUTE_TRADE_SIGNAL_DATABASE_OPERATION.CLEAR
        ? MINUTE_TRADE_SIGNAL_DATABASE_OPERATION.CLEAR
        : payload?.operation === MINUTE_TRADE_SIGNAL_DATABASE_OPERATION.THIN
            ? MINUTE_TRADE_SIGNAL_DATABASE_OPERATION.THIN
            : "";
    if (!operation) throw new Error("Невідома операція з базою маркерів");

    const identity = getBigBuySignalDatabaseIdentity(payload?.token);
    if (!identity) throw new Error("Не вдалося визначити ринок або монету");
    const runtime = state.bigBuySignalDatabase;
    if (runtime.maintenanceKeys.has(identity.key)) {
        throw new Error("Для цієї монети вже виконується операція з базою маркерів");
    }

    runtime.maintenanceKeys.add(identity.key);
    if (operation === MINUTE_TRADE_SIGNAL_DATABASE_OPERATION.CLEAR) {
        resetSelectedMinuteTradeSignalRuntimeForClear(identity);
    }

    let operationResult = null;
    let operationError = null;
    try {
        operationResult = operation === MINUTE_TRADE_SIGNAL_DATABASE_OPERATION.CLEAR
            ? await clearMinuteTradeSignalsForDatabaseIdentity(identity)
            : await thinMinuteTradeSignalsForDatabaseIdentity(identity);
    } catch (error) {
        operationError = error;
    }

    let synchronizationResult = null;
    try {
        synchronizationResult = await synchronizeMinuteTradeSignalDatabaseAfterMaintenance(
            identity,
            operation,
            operationResult || {
                deletedBuy: 0,
                deletedSell: 0,
                retainedBuy: 0,
                retainedSell: 0,
                scannedBuy: 0,
                scannedSell: 0
            },
            `database_${operation}`
        );
    } catch (error) {
        await recoverMinuteTradeSignalHistoryAfterMaintenanceFailure(
            identity,
            `database_${operation}_recovery`
        );
        releasePendingMinuteTradeSignalsWithoutDatabase(identity);
        runtime.maintenanceKeys.delete(identity.key);
        if (!operationError) operationError = error;
    }

    if (operationError) throw operationError;
    return {
        operation,
        market: identity.market,
        requestSymbol: identity.requestSymbol,
        ...operationResult,
        flushedInserted: synchronizationResult?.flushedInserted || 0,
        flushedDuplicate: synchronizationResult?.flushedDuplicate || 0
    };
}

async function cleanupExpiredBigBuySignalsFromDatabase(referenceMs = nowMs()) {
    const runtime = state.bigBuySignalDatabase;
    if (runtime.cleanupInFlight) return 0;
    runtime.cleanupInFlight = true;
    try {
        const database = await openBigBuySignalDatabase();
        if (!database) return 0;
        // V172: retention in IndexedDB is expanded to 30 days (2,592,000 seconds)
        // so that multi-day signals are preserved and not purged every 5 minutes.
        const safeReferenceMs = Number.isFinite(Number(referenceMs)) ? Number(referenceMs) : nowMs();
        const currentMinuteTime = Math.floor(safeReferenceMs / 60000) * BIG_BUY_SIGNAL_DATABASE.timeframeSeconds;
        const retentionSeconds = BIG_BUY_SIGNAL_DATABASE.retentionSeconds || (30 * 24 * 60 * 60);
        const minimumTime = currentMinuteTime - retentionSeconds;
        const transaction = database.transaction(BIG_BUY_SIGNAL_DATABASE.storeName, "readwrite");
        const completed = indexedDbTransactionToPromise(transaction);
        const index = transaction.objectStore(BIG_BUY_SIGNAL_DATABASE.storeName)
            .index(BIG_BUY_SIGNAL_DATABASE.timeIndex);
        const keyRange = IDBKeyRange.upperBound(minimumTime, true);
        let deleted = 0;
        await new Promise((resolve, reject) => {
            const request = index.openCursor(keyRange, "next");
            request.onerror = () => reject(request.error || new Error("IndexedDB cleanup cursor failed"));
            request.onsuccess = () => {
                const cursor = request.result;
                if (!cursor) {
                    resolve();
                    return;
                }
                const deleteRequest = cursor.delete();
                deleteRequest.onerror = () => reject(deleteRequest.error || new Error("IndexedDB cleanup delete failed"));
                deleteRequest.onsuccess = () => {
                    deleted += 1;
                    cursor.continue();
                };
            };
        });
        await completed;
        return deleted;
    } catch (error) {
        reportBigBuySignalDatabaseError("cleanup_minute_trade_signals", error);
        return 0;
    } finally {
        runtime.cleanupInFlight = false;
    }
}

function recordMinuteTradeSignal(tradeData, tradeTime, tradePrice, tradeQty, tradeNotional, threshold, side) {
    const normalizedSide = side === "sell" ? "sell" : "buy";
    const enabled = normalizedSide === "sell"
        ? isBigSellMinuteSignalRecordingEnabled()
        : isBigBuyMinuteSignalRecordingEnabled();
    if (!enabled) return false;
    if (!Number.isFinite(threshold) || threshold <= 0) return false;
    if (!Number.isFinite(tradeNotional) || tradeNotional < threshold) return false;

    const minuteTime = Math.floor(tradeTime / 60000) * BIG_BUY_SIGNAL_DATABASE.timeframeSeconds;
    if (!Number.isFinite(minuteTime) || minuteTime <= 0) return false;
    const runtimeField = normalizedSide === "sell"
        ? "lastBigSellMinuteSignalMinute"
        : "lastBigBuyMinuteSignalMinute";
    const identity = getBigBuySignalDatabaseIdentity(state.selectedToken);
    if (!identity) return false;
    const lastMinuteTime = Math.max(0, Number(state.scalpingState?.[runtimeField]) || 0);
    if (minuteTime <= lastMinuteTime) return false;

    const signalType = getMinuteTradeSignalType(normalizedSide);
    const signal = {
        id: String(tradeData?.a || `${signalType}_${identity.requestSymbol}_${minuteTime}`),
        time: minuteTime,
        tradeTime: Math.floor(tradeTime / 1000),
        price: tradePrice,
        qty: tradeQty,
        notional: tradeNotional,
        side: normalizedSide,
        signalType
    };

    state.scalpingState[runtimeField] = minuteTime;
    if (state.bigBuySignalDatabase.maintenanceKeys.has(identity.key)) {
        return queueMinuteTradeSignalDuringDatabaseMaintenance(identity, signal, normalizedSide);
    }

    post(normalizedSide === "sell" ? "BIG_SELL_MINUTE_SIGNAL" : "BIG_BUY_MINUTE_SIGNAL", {
        market: identity.market,
        requestSymbol: identity.requestSymbol,
        signal
    });

    void persistBigBuyMinuteSignalToDatabase(identity, signal, normalizedSide).then(result => {
        if (result.duplicate === true) {
            void loadAndPostBigBuyMinuteSignalHistory({
                market: identity.market,
                symbol: identity.symbol,
                requestSymbol: identity.requestSymbol
            }, `duplicate_${signalType}_database_key`);
        }
    });
    return true;
}

function recordBigBuyMinuteSignal(tradeData, tradeTime, tradePrice, tradeQty, tradeNotional, buyThreshold) {
    return recordMinuteTradeSignal(tradeData, tradeTime, tradePrice, tradeQty, tradeNotional, buyThreshold, "buy");
}

function recordBigSellMinuteSignal(tradeData, tradeTime, tradePrice, tradeQty, tradeNotional, sellThreshold) {
    return recordMinuteTradeSignal(tradeData, tradeTime, tradePrice, tradeQty, tradeNotional, sellThreshold, "sell");
}

const lastAggregateTradeIdByToken = new Map();
const FUTURES_AGG_TRADE_CATCHUP_PAGE_LIMIT = 20;
const FUTURES_AGG_TRADE_CATCHUP_BUFFER_LIMIT = 10000;

class MarketStreamWorkerManager {
    constructor(token) {
        this.token = token;
        this.market = getTokenMarket(token);
        this.tokenKey = getTokenKey(token);
        this.ws = null;
        this.closedByUser = false;
        this.reconnectAttempt = 0;
        this.reconnectTimer = null;
        this.openedAt = 0;
        this.subscribeId = null;
        this.subscribedStreams = [];
        this.connectionGeneration = 0;
        this.lastAggregateTradeId = Number(lastAggregateTradeIdByToken.get(this.tokenKey)) || 0;
        this.reconnectCatchupInProgress = false;
        this.bufferedAggTradesDuringCatchup = [];
        this.seenAggregateTradeIds = new Set();
        this.seenAggregateTradeIdQueue = [];
    }

    start() {
        this.closedByUser = false;
        this.reconnectAttempt = 0;
        updateWsDiagnostics("starting");
        updateWorkerStatus("stream_starting");
        this.connect();
    }

    stop(reason = "stopped") {
        this.closedByUser = true;
        this.connectionGeneration += 1;
        if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
        this.closeActiveWebSocket(1000, reason, { detachHandlers: true, clearReference: true });
        updateWsDiagnostics("stopped");
        updateSubscriptionDiagnostics("stopped");
        updateWorkerStatus("ready");
        postMetricsSnapshot("stream_stopped", true);
    }

    isCurrentConnection(ws, generation) {
        return !this.closedByUser && this.ws === ws && this.connectionGeneration === generation;
    }

    detachWebSocketHandlers(ws) {
        if (!ws) return;
        ws.onopen = null;
        ws.onmessage = null;
        ws.onerror = null;
        ws.onclose = null;
    }

    closeActiveWebSocket(code, reason, options = {}) {
        const ws = this.ws;
        if (!ws) return false;
        const { detachHandlers = false, clearReference = false } = options;
        if (detachHandlers) this.detachWebSocketHandlers(ws);
        if (clearReference && this.ws === ws) this.ws = null;
        if (ws.readyState === WebSocket.CLOSING || ws.readyState === WebSocket.CLOSED) return false;
        try {
            ws.close(code, String(reason || "closed").slice(0, 120));
            return true;
        } catch (error) {
            recordError("ws_close", error, { code, reason, readyState: ws.readyState });
            return false;
        }
    }

    connect() {
        if (this.closedByUser || state.paused || !state.pageVisible) return;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        this.closeActiveWebSocket(1000, "reconnect", { detachHandlers: true, clearReference: true });

        const streamSymbol = getStreamSymbol(this.token);
        const streamName = `${streamSymbol}@aggTrade`;
        const descriptor = createWebSocketStreamDescriptor(this.token, streamName, {
            route: FUTURES_WEBSOCKET_ROUTE.MARKET
        });
        const generation = this.connectionGeneration + 1;
        let ws;
        try {
            ws = new WebSocket(descriptor.url);
        } catch (error) {
            this.scheduleReconnect("WebSocket init error: " + error.message);
            return;
        }

        this.connectionGeneration = generation;
        this.ws = ws;

        ws.onopen = () => {
            if (!this.isCurrentConnection(ws, generation)) return;
            this.openedAt = nowMs();
            this.reconnectAttempt = 0;
            updateWsDiagnostics("open");
            const params = [descriptor.streamName];
            this.subscribedStreams = params;
            if (descriptor.mode === "subscribe") {
                this.subscribeId = Date.now();
                updateSubscriptionDiagnostics("pending", { id: this.subscribeId, streams: params });
                this.safeSend({ method: "SUBSCRIBE", params, id: this.subscribeId });
                post("LOG", { message: "SUBSCRIBE sent: " + params.join(", ") });
            } else {
                this.subscribeId = null;
                updateSubscriptionDiagnostics("confirmed", { id: null, streams: params });
                post("LOG", { message: `Direct ${getMarketLabel(this.market)} stream opened: ${descriptor.streamName}` });
            }
            updateWorkerStatus("online");
            postStreamStatus("connected", `Онлайн · ${getMarketLabel(this.market)}`);
            postMetricsSnapshot("stream_only_trades", true);
            const persistedAggregateTradeId = Number(lastAggregateTradeIdByToken.get(this.tokenKey)) || 0;
            if (persistedAggregateTradeId > this.lastAggregateTradeId) this.lastAggregateTradeId = persistedAggregateTradeId;
            if (this.market === MARKET.FUTURES && this.lastAggregateTradeId > 0) {
                this.startFuturesAggregateTradeCatchup(generation);
            }
        };
        ws.onmessage = event => {
            if (!this.isCurrentConnection(ws, generation)) return;
            this.handleMessageSafely(event);
        };
        ws.onerror = event => {
            if (!this.isCurrentConnection(ws, generation)) return;
            updateWsDiagnostics("error");
            updateWorkerStatus("stream_error");
            markStreamReceived("wsError");
            recordError("ws_error", new Error("WebSocket error event"), event?.message || null);
            postStreamStatus("error", "WebSocket помилка");
        };
        ws.onclose = event => {
            if (!this.isCurrentConnection(ws, generation)) return;
            if (this.ws === ws) this.ws = null;
            updateWsDiagnostics(`closed:${event.code}`);
            if (this.closedByUser || state.paused || !state.pageVisible) return;
            const reason = event.reason ? ` (${event.reason})` : "";
            this.scheduleReconnect(`WebSocket closed ${event.code}${reason}`);
        };
    }

    safeSend(payload) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            recordError("ws_send_not_open", new Error("WebSocket is not open"), { readyState: this.ws?.readyState, payload });
            return;
        }
        try {
            this.ws.send(JSON.stringify(payload));
        } catch (error) {
            recordError("ws_send", error, payload);
        }
    }

    handleMessageSafely(event) {
        try {
            this.handleMessage(event);
        } catch (error) {
            postError("ws_message_handler", error, event?.data);
        }
    }

    handleMessage(event) {
        let message;
        try {
            message = JSON.parse(event.data);
        } catch (error) {
            recordError("ws_json_parse", error, event?.data);
            return;
        }

        if (message && typeof message === "object" && Object.prototype.hasOwnProperty.call(message, "code")) {
            markStreamReceived("ack");
            updateSubscriptionDiagnostics("rejected", { id: message.id ?? this.subscribeId, streams: this.subscribedStreams });
            recordError("ws_subscribe_rejected", new Error(message.msg || "WebSocket request rejected"), message);
            return;
        }

        if (Object.prototype.hasOwnProperty.call(message, "result")) {
            markStreamReceived("ack");
            if (message.result === null && message.id === this.subscribeId) {
                updateSubscriptionDiagnostics("confirmed", { id: message.id, streams: this.subscribedStreams });
                post("LOG", { message: "SUBSCRIBE confirmed: " + this.subscribedStreams.join(", ") });
            } else if (message.result !== null) {
                updateSubscriptionDiagnostics("unexpected_ack", { id: message.id ?? this.subscribeId, streams: this.subscribedStreams });
                post("LOG", { message: "WS ACK result: " + safePayloadSample(message) });
            }
            return;
        }

        const data = message.data || message;
        if (!data || typeof data !== "object") {
            markStreamReceived("unknown");
            return;
        }
        const selectedSymbol = getRequestSymbol(this.token);
        if (data.s && data.s !== selectedSymbol) {
            markStreamRejected(data.e || "unknown", "symbol_mismatch");
            return;
        }
        if (data.e !== "aggTrade") {
            markStreamReceived("unknown");
            return;
        }
        markStreamReceived("aggTrade");
        this.handleAggTrade(data);
    }

    rememberAggregateTradeId(tradeId) {
        if (!Number.isInteger(tradeId) || tradeId <= 0) return true;
        const key = String(tradeId);
        if (this.seenAggregateTradeIds.has(key)) return false;
        this.seenAggregateTradeIds.add(key);
        this.seenAggregateTradeIdQueue.push(key);
        if (this.seenAggregateTradeIdQueue.length > 20000) {
            const removeCount = this.seenAggregateTradeIdQueue.length - 20000;
            for (let index = 0; index < removeCount; index += 1) this.seenAggregateTradeIds.delete(this.seenAggregateTradeIdQueue[index]);
            this.seenAggregateTradeIdQueue.splice(0, removeCount);
        }
        this.lastAggregateTradeId = Math.max(this.lastAggregateTradeId, tradeId);
        lastAggregateTradeIdByToken.set(this.tokenKey, this.lastAggregateTradeId);
        return true;
    }

    handleAggTrade(data, options = {}) {
        const tradeId = Number(data?.a);
        const tradePrice = Number(data?.p);
        const tradeQty = Number(data?.q);
        const tradeTime = Number(data?.T || data?.E) || nowMs();
        if (!Number.isFinite(tradePrice) || tradePrice <= 0 || !Number.isFinite(tradeQty) || tradeQty <= 0) {
            markStreamRejected("aggTrade", "invalid_trade");
            return;
        }
        if (this.reconnectCatchupInProgress && options.bypassCatchup !== true) {
            this.bufferedAggTradesDuringCatchup.push(data);
            if (this.bufferedAggTradesDuringCatchup.length > FUTURES_AGG_TRADE_CATCHUP_BUFFER_LIMIT) {
                this.bufferedAggTradesDuringCatchup.splice(0, this.bufferedAggTradesDuringCatchup.length - FUTURES_AGG_TRADE_CATCHUP_BUFFER_LIMIT);
            }
            return;
        }
        if (!this.rememberAggregateTradeId(tradeId)) return;

        markStreamApplied("aggTrade");
        const side = getAggTradeSide(data);
        post("AGG_TRADE", { trade: data, side, source: String(options.source || "live") });

        if (state.settings.scalping?.enabled) {
            const tradeNotional = tradePrice * tradeQty;
            const effectiveThresholds = getEffectiveTradeThresholds();
            // Threshold learning must use every valid market trade. Direction is applied only
            // after the two size levels have been determined, so an unknown aggressor side
            // must not remove an otherwise valid observation from either statistical model.
            recordAdvancedTradeThresholdSample(tradeNotional, effectiveThresholds, tradeTime);

            if (side === "unknown") {
                postMetricsSnapshot(options.source === "catchup" ? "aggTrade_catchup" : "aggTrade");
                return;
            }

            const deltaUsd = side === "buy" ? tradeNotional : -tradeNotional;
            if (state.settings.scalping.cvdEnabled) state.scalpingState.cvd += deltaUsd;

            const qualification = evaluateOneSecondBigTradeQualification(side, tradeNotional, effectiveThresholds);
            const isBigTrade = qualification.isBigTrade;
            if (qualification.shouldRecordMinuteBuy) {
                recordBigBuyMinuteSignal(
                    data,
                    tradeTime,
                    tradePrice,
                    tradeQty,
                    tradeNotional,
                    qualification.significantThresholdUsd
                );
            } else if (qualification.shouldRecordMinuteSell) {
                recordBigSellMinuteSignal(
                    data,
                    tradeTime,
                    tradePrice,
                    tradeQty,
                    tradeNotional,
                    qualification.significantThresholdUsd
                );
            }
            if (state.settings.scalping.bigTradesEnabled && isBigTrade) {
                state.scalpingState.bigTrades.push({
                    id: data.a || `${side}_${tradePrice}_${tradeQty}_${tradeTime}`,
                    time: Math.floor(tradeTime / 1000),
                    price: tradePrice,
                    qty: tradeQty,
                    notional: tradeNotional,
                    side,
                    isSignificant: qualification.isSignificantTrade
                });
                if (state.scalpingState.bigTrades.length > 50) state.scalpingState.bigTrades.shift();
            }
        }

        postMetricsSnapshot(options.source === "catchup" ? "aggTrade_catchup" : "aggTrade");
    }

    async startFuturesAggregateTradeCatchup(generation) {
        const persistedAggregateTradeId = Number(lastAggregateTradeIdByToken.get(this.tokenKey)) || 0;
        if (persistedAggregateTradeId > this.lastAggregateTradeId) this.lastAggregateTradeId = persistedAggregateTradeId;
        if (this.reconnectCatchupInProgress || this.market !== MARKET.FUTURES || this.lastAggregateTradeId <= 0) return;
        const config = getMarketConfig(MARKET.FUTURES);
        if (!config.aggTradesUrl) return;
        this.reconnectCatchupInProgress = true;
        this.bufferedAggTradesDuringCatchup = [];
        let nextId = this.lastAggregateTradeId + 1;
        let requiresFullResync = false;
        try {
            const symbol = encodeURIComponent(getRequestSymbol(this.token));
            for (let page = 0; page < FUTURES_AGG_TRADE_CATCHUP_PAGE_LIMIT; page += 1) {
                if (!this.isCurrentConnection(this.ws, generation)) return;
                const url = `${config.aggTradesUrl}?symbol=${symbol}&fromId=${nextId}&limit=1000`;
                const payload = await fetchJsonWithTimeout(url, {}, 10000);
                const trades = Array.isArray(payload) ? payload : [];
                if (trades.length === 0) break;
                trades.sort((left, right) => (Number(left?.a) || 0) - (Number(right?.a) || 0));
                let highestId = nextId - 1;
                for (const trade of trades) {
                    const id = Number(trade?.a);
                    if (!Number.isInteger(id) || id < nextId) continue;
                    highestId = Math.max(highestId, id);
                    this.handleAggTrade(trade, { bypassCatchup: true, source: "catchup" });
                }
                if (highestId < nextId || trades.length < 1000) break;
                nextId = highestId + 1;
                if (page === FUTURES_AGG_TRADE_CATCHUP_PAGE_LIMIT - 1) requiresFullResync = true;
            }
        } catch (error) {
            requiresFullResync = true;
            recordError("futures_agg_trade_catchup", error, { tokenKey: this.tokenKey, fromId: nextId });
        } finally {
            const buffered = this.bufferedAggTradesDuringCatchup.splice(0);
            this.reconnectCatchupInProgress = false;
            const stillActive = state.stream === this
                && !this.closedByUser
                && getTokenKey(state.selectedToken) === this.tokenKey;
            if (!stillActive) return;
            buffered.sort((left, right) => {
                const timeDifference = (Number(left?.T || left?.E) || 0) - (Number(right?.T || right?.E) || 0);
                if (timeDifference !== 0) return timeDifference;
                return (Number(left?.a) || 0) - (Number(right?.a) || 0);
            });
            for (const trade of buffered) this.handleAggTrade(trade, { bypassCatchup: true, source: "buffer" });
            if (requiresFullResync) {
                post("FUTURES_1S_RESYNC_REQUIRED", { tokenKey: this.tokenKey, reason: "catchup_limit_or_error" });
            } else {
                post("LOG", { message: `Futures aggTrade catch-up completed from ${nextId}` });
            }
        }
    }

    scheduleReconnect(reason) {
        if (this.closedByUser || state.paused || !state.pageVisible) return;
        this.connectionGeneration += 1;
        this.closeActiveWebSocket(1000, "reconnect", { detachHandlers: true, clearReference: true });
        post("LOG", { message: reason });
        postStreamStatus("connecting", "Reconnect...");
        updateWorkerStatus("recovering");
        const jitter = Math.floor(Math.random() * 350);
        const delay = Math.min(CONFIG.reconnectMaxMs, CONFIG.reconnectBaseMs * Math.pow(1.7, this.reconnectAttempt)) + jitter;
        this.reconnectAttempt += 1;
        if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            this.connect();
        }, delay);
    }
}

function resetRuntimeStores() {
    const previousRuntime = state.scalpingState?.advancedTradeThresholds;
    if (previousRuntime) {
        clearAdvancedTradeThresholdTimer(previousRuntime);
        clearAdvancedTradeThresholdProgressTimer(previousRuntime);
    }
    clearTursoThresholdFallbackTimer();
    state.tursoThresholdFallback.generation += 1;
    state.scalpingState = {
        cvd: 0,
        bigTrades: [],
        lastBigBuyMinuteSignalMinute: 0,
        lastBigSellMinuteSignalMinute: 0,
        lastPrice: null,
        lastSide: "buy",
        advancedTradeThresholds: createAdvancedTradeThresholdRuntime()
    };
}

function selectTokenInWorker(token, options = {}) {
    if (!token) return;
    if (state.stream) state.stream.stop("token changed");
    state.selectedToken = token;
    resetRuntimeStores();
    resetDiagnosticsForToken(token);
    postMetricsSnapshot("token_selected", true);
    void loadAndPostBigBuyMinuteSignalHistory(token, "token_selected");
    startTursoThresholdFallbackLifecycle("token_selected");
    if (options.start !== false && !state.paused && state.pageVisible) {
        state.stream = new MarketStreamWorkerManager(token);
        state.stream.start();
    }
}

function stopStream(reason = "stopped") {
    if (state.stream) state.stream.stop(reason);
    state.stream = null;
    resetAdvancedTradeThresholdWindow();
    updateWorkerStatus(state.pageVisible ? "ready" : "hidden");
}

function startStream() {
    if (!state.selectedToken || state.paused || !state.pageVisible) return;
    if (state.stream) state.stream.stop("restart");
    resetAdvancedTradeThresholdWindow({ reloadConfiguredThresholds: true });
    state.stream = new MarketStreamWorkerManager(state.selectedToken);
    state.stream.start();
}


function getWorkerDiagnosticsSnapshot() {
    return {
        ...state.workerDiagnostics,
        lastWorkerMessageAt: nowMs(),
        workerStatus: state.workerDiagnostics.workerStatus,
        wsState: state.diagnostics.wsState,
        subscriptionStatus: state.diagnostics.subscriptionStatus,
        streamDiagnostics: state.diagnostics,
        token: state.selectedToken ? {
            market: getTokenMarket(state.selectedToken),
            symbol: state.selectedToken.symbol,
            requestSymbol: state.selectedToken.requestSymbol,
            streamSymbol: getStreamSymbol(state.selectedToken),
            tokenKey: getTokenKey(state.selectedToken)
        } : null,
        wsOpenedAt: state.stream?.openedAt || null,
        wsLastMessageAt: state.diagnostics.streams.aggTrade.lastReceivedAt || null
    };
}

function updateWorkerStatus(status) {
    state.workerDiagnostics.workerStatus = status;
}

function postDiagnostics(reason = "diagnostics", options = {}) {
    const now = nowMs();
    const snapshot = getWorkerDiagnosticsSnapshot();
    const signature = getDiagnosticsSignature(snapshot);
    const force = options.force === true;
    if (!force) {
        const tooSoon = now - state.lastColdDiagnosticsPostAt < CONFIG.coldDiagnosticsMinIntervalMs;
        if (tooSoon || signature === state.lastColdDiagnosticsSignature) return;
    }
    state.lastColdDiagnosticsPostAt = now;
    state.lastColdDiagnosticsSignature = signature;
    post("WORKER_DIAGNOSTICS", { reasonCode: getMetricsReasonCode(reason), reason, diagnostics: snapshot });
}

function postStreamStatus(status, text) {
    post("STREAM_STATUS", { status, text });
    postDiagnostics("stream_status", { force: true });
}

function addChangedMetricsPart(payload, key, value, signature, force) {
    if (force || state.lastMetricsSignatures[key] !== signature) {
        payload[key] = value;
        state.lastMetricsSignatures[key] = signature;
        return true;
    }
    return false;
}







function postMetricsSnapshot(reason = "update", force = false) {
    const payload = {
        reasonCode: getMetricsReasonCode(reason),
        reason,
        reset: force === true
    };
    let changed = false;

    if (state.settings.scalping?.enabled) {
        const effectiveThresholds = getEffectiveTradeThresholds();
        const scalpingMetrics = {
            tokenKey: getTokenKey(state.selectedToken),
            cvd: state.settings.scalping.cvdEnabled ? state.scalpingState.cvd : 0,
            bigTrades: state.settings.scalping.bigTradesEnabled ? [...state.scalpingState.bigTrades] : [],
            thresholdSource: effectiveThresholds.source,
            bigTradesThresholdUsd: effectiveThresholds.bigTradesThresholdUsd,
            significantTradesThresholdUsd: effectiveThresholds.significantTradesThresholdUsd,
            significantTradesReady: effectiveThresholds.significantTradesReady,
            significantTradesConfidence: effectiveThresholds.significantTradesConfidence
        };
        changed = addChangedMetricsPart(payload, "scalping", scalpingMetrics, JSON.stringify(scalpingMetrics), force) || changed;
    } else {
        changed = addChangedMetricsPart(payload, "scalping", null, "null", force) || changed;
    }

    if (changed || force) post("METRICS_UPDATE", payload);
    postDiagnostics("metrics_cold");
}

function startWorkerTimers() {
    if (!state.diagnosticsTimer) {
        state.diagnosticsTimer = setInterval(() => {
            if (!state.pageVisible) return;
            postDiagnostics("diagnostics_timer");
        }, CONFIG.coldDiagnosticsMinIntervalMs);
    }
    if (!state.bigBuySignalDatabase.cleanupTimer) {
        state.bigBuySignalDatabase.cleanupTimer = setInterval(() => {
            if (!state.pageVisible) return;
            void cleanupExpiredBigBuySignalsFromDatabase();
        }, CONFIG.bigBuySignalDatabaseCleanupIntervalMs);
    }
    if (!state.tursoSync.flushTimer) {
        state.tursoSync.flushTimer = setInterval(() => {
            if (!state.pageVisible || !isTursoSyncConfigured()) return;
            void flushTursoSignalOutbox("minute_timer").catch(error => {
                postTursoSyncStatus("error", `Turso: ${error?.message || "помилка відправлення"}`, { error: error?.message || String(error) });
            });
        }, CONFIG.tursoOutboxFlushIntervalMs);
    }
}

function applySettings(settings) {
    if (!settings || typeof settings !== "object") return;
    const wasMinuteTradeSignalHistoryEnabled = isMinuteTradeSignalHistoryEnabled(state.settings);
    const previousScalping = state.settings?.scalping || DEFAULT_SCALPING_SETTINGS;
    const previousTursoConfig = sanitizeTursoSyncSettings(state.settings?.tursoSync);
    const previousThresholdFallbackConfigured = isTursoThresholdFallbackConfigured(previousTursoConfig);
    const previousRuntimeConfig = getConfiguredAdvancedThresholdRuntime(state.settings);
    const previousFirstLevelAlgorithmSignature = getAdvancedTradeThresholdFirstLevelAlgorithmSignature(state.settings);
    const previousSignificantAlgorithmSignature = getSignificantTradeTailAlgorithmSignature(state.settings);
    const next = { ...clonePlain(DEFAULT_SETTINGS), ...clonePlain(settings) };
    next.activeMarket = sanitizeMarket(next.activeMarket || getTokenMarket(state.selectedToken));
    next.tursoSync = sanitizeTursoSyncSettings(next.tursoSync);
    next.tursoTracking = next.tursoTracking && typeof next.tursoTracking === "object" ? clonePlain(next.tursoTracking) : {};
    const incomingScalping = next.scalping && typeof next.scalping === "object"
        ? next.scalping
        : {};
    next.scalping = {
        ...clonePlain(getDefaultScalpingSettingsForMarket(next.activeMarket)),
        ...incomingScalping
    };
    next.scalping.bigTradesThresholdUsd = clampNumber(
        next.scalping.bigTradesThresholdUsd,
        100,
        10000000,
        DEFAULT_SCALPING_SETTINGS.bigTradesThresholdUsd
    );
    next.scalping.significantTradesThresholdUsd = Math.max(
        next.scalping.bigTradesThresholdUsd,
        clampNumber(
            next.scalping.significantTradesThresholdUsd ?? next.scalping.bigBuysThresholdUsd,
            100,
            10000000,
            DEFAULT_SCALPING_SETTINGS.significantTradesThresholdUsd
        )
    );
    delete next.scalping.bigBuysThresholdUsd;
    next.scalping.autoAlertsMinDistancePercent = clampNumber(
        next.scalping.autoAlertsMinDistancePercent,
        0,
        10,
        DEFAULT_SCALPING_SETTINGS.autoAlertsMinDistancePercent
    );
    next.scalping.advancedTradeThresholdsEnabled = next.scalping.advancedTradeThresholdsEnabled === true;
    next.scalping.bigBuyMinuteMarkersEnabled = next.scalping.bigBuyMinuteMarkersEnabled !== false;
    next.scalping.bigBuyFiveMinuteMarkersEnabled = next.scalping.bigBuyFiveMinuteMarkersEnabled === true;
    next.scalping.bigSellMinuteMarkersEnabled = next.scalping.bigSellMinuteMarkersEnabled !== false;
    Object.assign(
        next.scalping,
        sanitizeAdvancedTradeThresholdAlgorithmSettings(incomingScalping, next.activeMarket)
    );
    state.settings = next;
    const nextTursoConfig = sanitizeTursoSyncSettings(next.tursoSync);
    const nextThresholdFallbackConfigured = isTursoThresholdFallbackConfigured(nextTursoConfig);
    const thresholdFallbackConfigurationChanged =
        previousThresholdFallbackConfigured !== nextThresholdFallbackConfigured
        || previousTursoConfig.databaseUrl !== nextTursoConfig.databaseUrl
        || previousTursoConfig.authToken !== nextTursoConfig.authToken
        || previousTursoConfig.thresholdFallbackEnabled !== nextTursoConfig.thresholdFallbackEnabled;
    if (previousTursoConfig.databaseUrl !== nextTursoConfig.databaseUrl || previousTursoConfig.authToken !== nextTursoConfig.authToken) {
        state.tursoSync.schemaReadyKey = "";
    }

    const nextRuntimeConfig = getConfiguredAdvancedThresholdRuntime(next);
    const nextFirstLevelAlgorithmSignature = getAdvancedTradeThresholdFirstLevelAlgorithmSignature(next);
    const nextSignificantAlgorithmSignature = getSignificantTradeTailAlgorithmSignature(next);
    const firstLevelAlgorithmSettingsChanged = previousFirstLevelAlgorithmSignature !== nextFirstLevelAlgorithmSignature;
    const significantAlgorithmSettingsChanged = previousSignificantAlgorithmSignature !== nextSignificantAlgorithmSignature;
    const settingsModeChanged = previousScalping.advancedTradeThresholdsEnabled !== next.scalping.advancedTradeThresholdsEnabled;
    const runtimeIdentityChanged = previousRuntimeConfig.market !== nextRuntimeConfig.market
        || previousRuntimeConfig.requestSymbol !== nextRuntimeConfig.requestSymbol;
    if (state.scalpingState?.advancedTradeThresholds) {
        const runtime = state.scalpingState.advancedTradeThresholds;
        const applyConfiguredRuntimeThresholds = () => {
            runtime.configuredFallbackBigTradesThresholdUsd = nextRuntimeConfig.bigTradesThresholdUsd;
            runtime.configuredFallbackSignificantTradesThresholdUsd = nextRuntimeConfig.significantTradesThresholdUsd;
            if (nextRuntimeConfig.updatedAt < runtime.updatedAt) return;
            if (!runtime.tursoFallbackLargeActive || runtime.bigTradesAutoReady) {
                runtime.bigTradesThresholdUsd = nextRuntimeConfig.bigTradesThresholdUsd;
            }
            if (!runtime.tursoFallbackSignificantActive || runtime.significantTradesAutoReady) {
                runtime.significantTradesThresholdUsd = Math.max(
                    runtime.bigTradesThresholdUsd,
                    nextRuntimeConfig.significantTradesThresholdUsd
                );
            }
            runtime.significantTradesReady = runtime.significantTradesThresholdUsd > runtime.bigTradesThresholdUsd;
            runtime.significantTradesConfidence = nextRuntimeConfig.significantTradesConfidence;
            runtime.updatedAt = nextRuntimeConfig.updatedAt;
        };

        if (settingsModeChanged || runtimeIdentityChanged) {
            runtime.bigTradesAutoReady = false;
            runtime.significantTradesAutoReady = false;
            runtime.tursoFallbackLargeActive = false;
            runtime.tursoFallbackSignificantActive = false;
            resetAdvancedTradeThresholdWindow({ reloadConfiguredThresholds: true });
        } else if (firstLevelAlgorithmSettingsChanged) {
            runtime.bigTradesAutoReady = false;
            runtime.tursoFallbackLargeActive = false;
            // First-level estimator changes invalidate only the all-trades window.
            // Relative-strength observations remain valid for both tail estimators.
            resetAdvancedTradeThresholdWindow({
                reloadConfiguredThresholds: true,
                preserveSignificantTradeHistory: true
            });
        } else {
            applyConfiguredRuntimeThresholds();
            if (significantAlgorithmSettingsChanged) {
                runtime.significantTradesAutoReady = false;
                runtime.tursoFallbackSignificantActive = false;
                // Tail-model selection and parameters can be applied to the existing
                // observations immediately; neither trade reservoir needs to be cleared.
                postAdvancedTradeThresholdProgress(runtime, "significant_settings_changed");
            }
        }
    }

    if (!nextThresholdFallbackConfigured) {
        if (thresholdFallbackConfigurationChanged) state.tursoThresholdFallback.generation += 1;
        deactivateTursoThresholdFallback("settings_disabled");
    } else if (state.selectedToken && thresholdFallbackConfigurationChanged) {
        // Invalidate any read started with the previous URL/token/feature state.
        clearTursoThresholdFallbackTimer();
        state.tursoThresholdFallback.generation += 1;
        reconcileTursoThresholdFallbackSchedule("turso_threshold_settings_changed", { immediate: true });
    } else {
        reconcileTursoThresholdFallbackSchedule("settings_reconciled");
    }

    const minuteTradeSignalHistoryEnabled = isMinuteTradeSignalHistoryEnabled(next);
    if (state.selectedToken && !wasMinuteTradeSignalHistoryEnabled && minuteTradeSignalHistoryEnabled) {
        void loadAndPostBigBuyMinuteSignalHistory(state.selectedToken, "scalping_enabled");
    }
}

const handleMainWorkerMessage = event => {
    const message = event.data || {};
    const { type, requestId } = message;
    const payload = hydrateIncomingPayload(message.payload || {});
    state.workerDiagnostics.lastMainMessageAt = nowMs();

    Promise.resolve()
        .then(async () => {
            switch (type) {
                case "INIT": {
                    applySettings(payload.settings || DEFAULT_SETTINGS);
                    state.paused = payload.paused === true;
                    state.pageVisible = payload.pageVisible !== false;
                    state.workerDiagnostics.workerGeneration = Number(payload.workerGeneration) || 0;
                    if (payload.indicatorsOn1sEnabled !== undefined) {
                        state.indicatorsOn1sEnabled = payload.indicatorsOn1sEnabled === true;
                    }
                    abortAllChartRequests("worker_reinitialized");
                    resetRuntimeStores();
                    startWorkerTimers();
                    updateWorkerStatus("ready");
                    reply(requestId, true, { diagnostics: getWorkerDiagnosticsSnapshot() });
                    post("HELLO", { diagnostics: getWorkerDiagnosticsSnapshot() });
                    return;
                }
                case "PING": {
                    const sentAt = Number(payload.sentAt) || nowMs();
                    const latency = Math.max(0, nowMs() - sentAt);
                    state.workerDiagnostics.lastPingAt = sentAt;
                    state.workerDiagnostics.lastPongAt = nowMs();
                    state.workerDiagnostics.lastPongLatencyMs = latency;
                    post("PONG", { requestId, sentAt, receivedAt: nowMs(), latencyMs: latency });
                    return;
                }
                case "LOAD_TOKENS": {
                    const market = sanitizeMarket(payload.market || state.settings.activeMarket);
                    const tokens = await loadTokensInWorker(market, { forceRefresh: payload.forceRefresh === true });
                    const saved = String(payload.selectedRequestSymbol || "").trim().toUpperCase();
                    const selected = tokens.find(token => getRequestSymbol(token) === saved) || tokens[0];
                    reply(requestId, true, { market, tokens, selected });
                    return;
                }
                case "UPDATE_SETTINGS": {
                    applySettings(payload.settings || state.settings);
                    if (payload.indicatorsOn1sEnabled !== undefined) {
                        state.indicatorsOn1sEnabled = payload.indicatorsOn1sEnabled === true;
                    }
                    if (payload.restartStream === true && state.selectedToken && !state.paused && state.pageVisible) {
                        startStream();
                    }
                    reply(requestId, true, { diagnostics: getWorkerDiagnosticsSnapshot() });
                    postMetricsSnapshot("settings_updated", true);
                    return;
                }
                case "SELECT_TOKEN": {
                    applySettings(payload.settings || state.settings);
                    state.chartHistoryModes.clear();
                    state.paused = payload.paused === true;
                    if (payload.indicatorsOn1sEnabled !== undefined) {
                        state.indicatorsOn1sEnabled = payload.indicatorsOn1sEnabled === true;
                    }
                    selectTokenInWorker(payload.token, { start: payload.start !== false });
                    reply(requestId, true, { diagnostics: getWorkerDiagnosticsSnapshot() });
                    return;
                }
                case "MAINTAIN_MINUTE_TRADE_SIGNAL_DATABASE": {
                    const result = await maintainMinuteTradeSignalDatabase(payload);
                    reply(requestId, true, result);
                    return;
                }
                case "TEST_TURSO_CONNECTION": {
                    const result = await testTursoConnection(payload.tursoSync);
                    reply(requestId, true, result);
                    return;
                }
                case "SYNC_TURSO_SIGNALS": {
                    const result = await synchronizeTursoSignals(payload);
                    reply(requestId, true, result);
                    return;
                }
                case "SYNC_TURSO_TOKEN_SIGNALS": {
                    const result = await synchronizeTursoTokenSignals(payload);
                    reply(requestId, true, result);
                    return;
                }
                case "RELOAD_MINUTE_TRADE_SIGNAL_HISTORY": {
                    const result = await reloadMinuteTradeSignalHistory(payload);
                    reply(requestId, true, result);
                    return;
                }
                case "START_STREAM": {
                    state.paused = false;
                    if (payload.token) state.selectedToken = payload.token;
                    applySettings(payload.settings || state.settings);
                    startStream();
                    reply(requestId, true, { diagnostics: getWorkerDiagnosticsSnapshot() });
                    return;
                }
                case "STOP_STREAM": {
                    stopStream(payload.reason || "stopped");
                    reply(requestId, true, { diagnostics: getWorkerDiagnosticsSnapshot() });
                    return;
                }
                case "PAGE_HIDDEN": {
                    state.pageVisible = false;
                    state.workerDiagnostics.workerHiddenSince = nowMs();
                    stopStream(payload.reason || "page hidden");
                    updateWorkerStatus("hidden");
                    reply(requestId, true, { diagnostics: getWorkerDiagnosticsSnapshot() });
                    return;
                }
                case "PAGE_VISIBLE": {
                    state.pageVisible = true;
                    state.workerDiagnostics.workerResumeCount += 1;
                    state.workerDiagnostics.workerHiddenSince = null;
                    updateWorkerStatus("ready");
                    if (state.selectedToken) {
                        void loadAndPostBigBuyMinuteSignalHistory(state.selectedToken, "page_visible");
                        // Threshold fallback already has its own 5m02s lifecycle.
                        // Do not add a second SELECT merely because the tab became visible.
                        reconcileTursoThresholdFallbackSchedule("page_visible");
                    }
                    if (payload.resumeStream !== false && state.selectedToken && !state.paused) startStream();
                    reply(requestId, true, { diagnostics: getWorkerDiagnosticsSnapshot() });
                    return;
                }
                case "CANCEL_CHART_REQUEST": {
                    const cancelled = abortChartRequest(payload.chartId, payload.chartGeneration, payload.reason || "main_cancelled");
                    reply(requestId, true, { cancelled, chartId: Number(payload.chartId), chartGeneration: Number(payload.chartGeneration) || 0 });
                    return;
                }
                case "SET_CHART_HISTORY_MODE": {
                    const chartId = Number(payload.chartId);
                    const enabled = payload.enabled === true;
                    abortChartRequest(chartId, null, enabled ? "history_mode_enabled" : "history_mode_disabled");
                    setChartHistoryMode(chartId, enabled);
                    reply(requestId, true, { chartId, enabled });
                    return;
                }
                case "LOAD_CHART_INITIAL": {
                    applySettings(payload.settings || state.settings);
                    if (payload.indicatorsOn1sEnabled !== undefined) {
                        state.indicatorsOn1sEnabled = payload.indicatorsOn1sEnabled === true;
                    }
                    if (payload.timeframe === "1m") {
                        void loadAndPostBigBuyMinuteSignalHistory(payload.token, "chart_1m_initial_load");
                    }
                    const result = await loadChartInitial(payload);
                    replyChartResult(requestId, result);
                    return;
                }
                case "LOAD_CHART_HISTORY_WINDOW": {
                    applySettings(payload.settings || state.settings);
                    if (payload.indicatorsOn1sEnabled !== undefined) {
                        state.indicatorsOn1sEnabled = payload.indicatorsOn1sEnabled === true;
                    }
                    const result = await loadChartHistoryWindow(payload);
                    replyChartResult(requestId, result);
                    return;
                }
                case "LOAD_BIG_BUY_FIVE_MINUTE_SIGNAL_SOURCE_RANGE": {
                    const result = await loadBigBuyFiveMinuteSignalSourceRange(payload);
                    reply(requestId, true, result);
                    return;
                }
                case "REFRESH_CHART_TAIL": {
                    applySettings(payload.settings || state.settings);
                    if (payload.indicatorsOn1sEnabled !== undefined) {
                        state.indicatorsOn1sEnabled = payload.indicatorsOn1sEnabled === true;
                    }
                    const result = await refreshChartTail(payload);
                    replyChartResult(requestId, result);
                    return;
                }
                case "RECALCULATE_CHART": {
                    applySettings(payload.settings || state.settings);
                    if (payload.indicatorsOn1sEnabled !== undefined) {
                        state.indicatorsOn1sEnabled = payload.indicatorsOn1sEnabled === true;
                    }
                    const result = recalculateChart(payload);
                    replyChartResult(requestId, result);
                    return;
                }
case "REQUEST_DIAGNOSTICS": {
                    reply(requestId, true, { diagnostics: getWorkerDiagnosticsSnapshot() });
                    postDiagnostics(payload.reason || "main_request");
                    return;
                }
                default:
                    throw new Error("Unknown worker message type: " + type);
            }
        })
        .catch(error => {
            const errorName = String(error?.name || "Error");
            const expectedAbort = errorName === "AbortError";
            if (!expectedAbort) postError(type || "worker_message", error, payload);
            if (requestId !== undefined && requestId !== null) {
                reply(requestId, false, {
                    message: error?.message || String(error || "Worker error"),
                    name: errorName,
                    code: error?.code || (expectedAbort ? "CHART_REQUEST_ABORTED" : "")
                });
            }
        });
};

self.onmessage = event => {
    const type = event?.data?.type;
    const isOrderBookMessage = typeof type === "string" && type.startsWith("ORDERBOOK_");
    if (isOrderBookMessage || orderBookProfileRuntime !== null) {
        handleOrderBookWorkerMessage(event);
        return;
    }
    handleMainWorkerMessage(event);
};

updateWorkerStatus("ready");
post("HELLO", { diagnostics: getWorkerDiagnosticsSnapshot() });


function computeCvdAndAdvancedMetrics(data, token = state.selectedToken) {
    const round4 = (v) => Math.round(v * 10000) / 10000;
    const SIGNAL_VERSION = 2;
    const REASON_DELTA_DIVERGENCE = 1;
    const REASON_CVD_BAND_EXTREME = 2;
    const REASON_PRICE_REJECTION = 4;
    const REASON_VOLUME_ANOMALY = 8;
    const REASON_LOCAL_EXTREME = 16;

    const clamp01 = (value) => {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) return 0;
        if (numeric <= 0) return 0;
        if (numeric >= 1) return 1;
        return numeric;
    };
    const clampScore = (value) => {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) return 0;
        return Math.max(0, Math.min(100, numeric));
    };
    const finiteOrZero = (value) => Number.isFinite(Number(value)) ? Number(value) : 0;
    const resetAdvancedFields = (candle, resetCvd = false) => {
        if (!candle) return;
        if (resetCvd) candle.cvd = null;
        candle.cvdBBMiddle = null;
        candle.cvdBBUpper = null;
        candle.cvdBBLower = null;
        candle.cvdDivSignal = null;
        candle.cvdDivSignalCode = 0;
        candle.cvdSignalVersion = 0;
        candle.cvdSignalStrength = 0;
        candle.cvdSignalConfidenceCode = 0;
        candle.cvdSignalReasonCode = 0;
        candle.cvdDeltaZ = 0;
        candle.cvdVolumeZ = 0;
        candle.cvdPriceReactionScore = 0;
        candle.cvdLocationScore = 0;
        candle.cvdBandScore = 0;
    };
    const clearSignalOnly = (candle) => {
        if (!candle) return;
        candle.cvdDivSignal = null;
        candle.cvdDivSignalCode = 0;
        candle.cvdSignalVersion = 0;
        candle.cvdSignalStrength = 0;
        candle.cvdSignalConfidenceCode = 0;
        candle.cvdSignalReasonCode = 0;
        candle.cvdDeltaZ = 0;
        candle.cvdVolumeZ = 0;
        candle.cvdPriceReactionScore = 0;
        candle.cvdLocationScore = 0;
        candle.cvdBandScore = 0;
    };

    if (!state.settings.scalping?.enabled || !state.settings.scalping.cvdEnabled) {
        for (let i = 0; i < data.length; i += 1) resetAdvancedFields(data[i], true);
        return;
    }

    const s = state.settings.scalping;
    const cvdAdvancedEnabled = s.cvdAdvancedEnabled === true;
    const deltas = new Array(data.length);
    const notionalVolumes = new Array(data.length);

    for (let i = 0; i < data.length; i += 1) {
        const candle = data[i];
        const takerBuy = finiteOrZero(candle.takerBuyVolume);
        const vol = finiteOrZero(candle.volume);
        let delta = 2 * takerBuy - vol;
        if (vol > 0 && (takerBuy <= 0 || Math.abs(takerBuy - vol) < 1e-5)) {
            delta = candle.close > candle.open ? vol : (candle.close < candle.open ? -vol : 0);
        }
        const price = finiteOrZero(candle.close);
        deltas[i] = delta * price;
        notionalVolumes[i] = Math.abs(vol * price);
    }

    if (cvdAdvancedEnabled && s.cvdRollingEnabled) {
        const cvdRollingWindow = Math.max(10, Math.min(2000, Number(s.cvdRollingWindow) || 200));
        let runningSum = 0;
        for (let i = 0; i < data.length; i += 1) {
            runningSum += deltas[i];
            if (i >= cvdRollingWindow) runningSum -= deltas[i - cvdRollingWindow];
            data[i].cvd = runningSum;
        }
    } else {
        let cvdAccumulator = 0;
        for (let i = 0; i < data.length; i += 1) {
            cvdAccumulator += deltas[i];
            data[i].cvd = cvdAccumulator;
        }
    }

    if (cvdAdvancedEnabled && s.cvdBandsEnabled) {
        const cvdPeriod = Math.max(5, Math.min(500, Number(s.cvdBandsPeriod) || 20));
        const cvdMult = Math.max(0.1, Math.min(10.0, Number(s.cvdBandsMult) || 2.0));
        const anchor = finiteOrZero(data[0]?.cvd);
        let sumDiff = 0;
        let sumSqDiff = 0;
        for (let i = 0; i < data.length; i += 1) {
            const diff = finiteOrZero(data[i].cvd) - anchor;
            sumDiff += diff;
            sumSqDiff += diff * diff;
            if (i >= cvdPeriod) {
                const oldDiff = finiteOrZero(data[i - cvdPeriod].cvd) - anchor;
                sumDiff -= oldDiff;
                sumSqDiff -= oldDiff * oldDiff;
            }
            if (i < cvdPeriod - 1) {
                data[i].cvdBBMiddle = null;
                data[i].cvdBBUpper = null;
                data[i].cvdBBLower = null;
                continue;
            }
            const variance = Math.max(0, (sumSqDiff - (sumDiff * sumDiff) / cvdPeriod) / cvdPeriod);
            const stdDev = Math.sqrt(variance);
            const mean = (sumDiff / cvdPeriod) + anchor;
            data[i].cvdBBMiddle = mean;
            data[i].cvdBBUpper = mean + cvdMult * stdDev;
            data[i].cvdBBLower = mean - cvdMult * stdDev;
        }
    } else {
        for (let i = 0; i < data.length; i += 1) {
            data[i].cvdBBMiddle = null;
            data[i].cvdBBUpper = null;
            data[i].cvdBBLower = null;
        }
    }

    if (cvdAdvancedEnabled && s.cvdDivergenceEnabled) {
        const L = 3;
        const divMult = Math.max(0.1, Math.min(5.0, Number(s.cvdDivergenceThreshold) || 1.5));
        const cvdBandPeriod = Math.max(5, Math.min(500, Number(s.cvdBandsPeriod) || 20));
        const statWindow = Math.max(30, Math.min(200, Math.round(cvdBandPeriod * 2.5)));
        const levelLookback = Math.max(12, Math.min(120, Math.round((Number(s.cvdRollingWindow) || 200) * 0.15)));
        const minDeltaZ = Math.max(0.75, Math.min(3.5, divMult * 0.85));
        const minVolumeZ = Math.max(0.05, Math.min(2.5, divMult * 0.35));
        const minStrength = Math.max(48, Math.min(78, 45 + divMult * 8));
        const cooldownBars = Math.max(3, Math.min(10, Math.round(3 + divMult * 1.5)));
        const cooldownReplaceGain = 10;

        let deltaSum = 0;
        let deltaSqSum = 0;
        let volumeSum = 0;
        let volumeSqSum = 0;
        const lowDeque = [];
        const highDeque = [];
        const lastSignalByDirection = {
            1: { index: -Infinity, strength: 0 },
            "-1": { index: -Infinity, strength: 0 }
        };

        for (let i = 0; i < data.length; i += 1) {
            const candle = data[i];
            clearSignalOnly(candle);
            candle.cvdSignalVersion = SIGNAL_VERSION;

            const deltaValue = deltas[i];
            const volumeValue = notionalVolumes[i];
            deltaSum += deltaValue;
            deltaSqSum += deltaValue * deltaValue;
            volumeSum += volumeValue;
            volumeSqSum += volumeValue * volumeValue;
            if (i >= statWindow) {
                const oldDelta = deltas[i - statWindow];
                const oldVolume = notionalVolumes[i - statWindow];
                deltaSum -= oldDelta;
                deltaSqSum -= oldDelta * oldDelta;
                volumeSum -= oldVolume;
                volumeSqSum -= oldVolume * oldVolume;
            }

            while (lowDeque.length && lowDeque[0] <= i - levelLookback) lowDeque.shift();
            while (highDeque.length && highDeque[0] <= i - levelLookback) highDeque.shift();
            while (lowDeque.length && finiteOrZero(data[lowDeque[lowDeque.length - 1]].low) >= finiteOrZero(candle.low)) lowDeque.pop();
            while (highDeque.length && finiteOrZero(data[highDeque[highDeque.length - 1]].high) <= finiteOrZero(candle.high)) highDeque.pop();
            lowDeque.push(i);
            highDeque.push(i);

            if (i < L + 5) continue;

            const statsCount = Math.min(i + 1, statWindow);
            const deltaMean = deltaSum / statsCount;
            const deltaVariance = Math.max(0, (deltaSqSum - (deltaSum * deltaSum) / statsCount) / statsCount);
            const deltaStd = Math.sqrt(deltaVariance);
            const volumeMean = volumeSum / statsCount;
            const volumeVariance = Math.max(0, (volumeSqSum - (volumeSum * volumeSum) / statsCount) / statsCount);
            const volumeStd = Math.sqrt(volumeVariance);
            const volumeZ = volumeStd > 0 ? Math.max(0, (volumeValue - volumeMean) / volumeStd) : 0;

            const priceChange = finiteOrZero(candle.close) - finiteOrZero(data[i - L].close);
            const cvdChange = finiteOrZero(candle.cvd) - finiteOrZero(data[i - L].cvd);
            const threshold = Math.max(deltaStd * Math.sqrt(L) * divMult, Math.abs(deltaMean) * 0.05);
            const directionalDeltaZ = threshold > 0 ? cvdChange / threshold : 0;
            const deltaAbsZ = Math.abs(directionalDeltaZ);

            const range = Math.max(finiteOrZero(candle.high) - finiteOrZero(candle.low), Math.abs(finiteOrZero(candle.close)) * 1e-8, 1e-12);
            const closePos = clamp01((finiteOrZero(candle.close) - finiteOrZero(candle.low)) / range);
            const lowerWickRatio = clamp01((Math.min(finiteOrZero(candle.open), finiteOrZero(candle.close)) - finiteOrZero(candle.low)) / range);
            const upperWickRatio = clamp01((finiteOrZero(candle.high) - Math.max(finiteOrZero(candle.open), finiteOrZero(candle.close))) / range);
            const bullishPriceReaction = clamp01(closePos * 0.62 + lowerWickRatio * 0.38);
            const bearishPriceReaction = clamp01((1 - closePos) * 0.62 + upperWickRatio * 0.38);

            const localLow = finiteOrZero(data[lowDeque[0]]?.low);
            const localHigh = finiteOrZero(data[highDeque[0]]?.high);
            const localRange = Math.max(localHigh - localLow, Math.abs(finiteOrZero(candle.close)) * 1e-8, 1e-12);
            const bullishLocationScore = clamp01(1 - ((finiteOrZero(candle.low) - localLow) / (localRange * 0.45)));
            const bearishLocationScore = clamp01(1 - ((localHigh - finiteOrZero(candle.high)) / (localRange * 0.45)));

            const hasBands = Number.isFinite(candle.cvdBBMiddle) && Number.isFinite(candle.cvdBBUpper) && Number.isFinite(candle.cvdBBLower);
            const bandHalfWidth = hasBands ? Math.max(Math.abs(candle.cvdBBUpper - candle.cvdBBMiddle), Math.abs(candle.cvdBBMiddle - candle.cvdBBLower), 1e-12) : 0;
            const bullishBandScore = hasBands ? clamp01((candle.cvdBBMiddle - finiteOrZero(candle.cvd)) / bandHalfWidth) : 0;
            const bearishBandScore = hasBands ? clamp01((finiteOrZero(candle.cvd) - candle.cvdBBMiddle) / bandHalfWidth) : 0;

            const bullishDeltaScore = (cvdChange < 0 && priceChange >= -range * 0.15) ? clamp01((deltaAbsZ - minDeltaZ) / Math.max(1, divMult)) : 0;
            const bearishDeltaScore = (cvdChange > 0 && priceChange <= range * 0.15) ? clamp01((deltaAbsZ - minDeltaZ) / Math.max(1, divMult)) : 0;
            const volumeScore = clamp01((volumeZ - minVolumeZ) / Math.max(1, 3 - minVolumeZ));

            const bullishBase = Math.max(bullishDeltaScore, bullishBandScore * (finiteOrZero(candle.close) >= finiteOrZero(candle.open) ? 1 : 0.65));
            const bearishBase = Math.max(bearishDeltaScore, bearishBandScore * (finiteOrZero(candle.close) <= finiteOrZero(candle.open) ? 1 : 0.65));
            const bullishStrength = clampScore(100 * (0.36 * bullishBase + 0.24 * bullishPriceReaction + 0.18 * volumeScore + 0.16 * bullishLocationScore + 0.06 * clamp01(deltaAbsZ / 4)));
            const bearishStrength = clampScore(100 * (0.36 * bearishBase + 0.24 * bearishPriceReaction + 0.18 * volumeScore + 0.16 * bearishLocationScore + 0.06 * clamp01(deltaAbsZ / 4)));

            let direction = 0;
            let strength = 0;
            let priceReactionScore = 0;
            let locationScore = 0;
            let bandScore = 0;
            let reason = 0;

            if (bullishBase > 0 && bullishStrength >= minStrength && bullishStrength >= bearishStrength) {
                direction = 1;
                strength = bullishStrength;
                priceReactionScore = bullishPriceReaction;
                locationScore = bullishLocationScore;
                bandScore = bullishBandScore;
                if (bullishDeltaScore >= 0.35) reason |= REASON_DELTA_DIVERGENCE;
                if (bullishBandScore >= 0.75) reason |= REASON_CVD_BAND_EXTREME;
                if (bullishPriceReaction >= 0.55) reason |= REASON_PRICE_REJECTION;
                if (volumeScore >= 0.25) reason |= REASON_VOLUME_ANOMALY;
                if (bullishLocationScore >= 0.55) reason |= REASON_LOCAL_EXTREME;
            } else if (bearishBase > 0 && bearishStrength >= minStrength) {
                direction = -1;
                strength = bearishStrength;
                priceReactionScore = bearishPriceReaction;
                locationScore = bearishLocationScore;
                bandScore = bearishBandScore;
                if (bearishDeltaScore >= 0.35) reason |= REASON_DELTA_DIVERGENCE;
                if (bearishBandScore >= 0.75) reason |= REASON_CVD_BAND_EXTREME;
                if (bearishPriceReaction >= 0.55) reason |= REASON_PRICE_REJECTION;
                if (volumeScore >= 0.25) reason |= REASON_VOLUME_ANOMALY;
                if (bearishLocationScore >= 0.55) reason |= REASON_LOCAL_EXTREME;
            }

            candle.cvdDeltaZ = Number.isFinite(directionalDeltaZ) ? round4(directionalDeltaZ) : 0;
            candle.cvdVolumeZ = Number.isFinite(volumeZ) ? round4(volumeZ) : 0;
            candle.cvdPriceReactionScore = round4(priceReactionScore);
            candle.cvdLocationScore = round4(locationScore);
            candle.cvdBandScore = round4(bandScore);

            if (direction === 0 || reason === 0) continue;

            const cluster = lastSignalByDirection[direction];
            if (i - cluster.index <= cooldownBars) {
                if (strength <= cluster.strength + cooldownReplaceGain) {
                    continue;
                }
                clearSignalOnly(data[cluster.index]);
            }

            const roundedStrength = Math.round(strength);
            candle.cvdDivSignalCode = direction;
            candle.cvdDivSignal = direction > 0 ? "bullish_absorption" : "bearish_absorption";
            candle.cvdSignalVersion = SIGNAL_VERSION;
            candle.cvdSignalStrength = roundedStrength;
            candle.cvdSignalConfidenceCode = roundedStrength >= 82 && volumeScore >= 0.35 && priceReactionScore >= 0.55 ? 3 : (roundedStrength >= 66 ? 2 : 1);
            candle.cvdSignalReasonCode = reason;
            lastSignalByDirection[direction] = { index: i, strength: roundedStrength };
        }
    } else {
        for (let i = 0; i < data.length; i += 1) clearSignalOnly(data[i]);
    }
}


// -----------------------------------------------------------------------------
// V97 hybrid order-book profile role.
// The same physical worker file is instantiated a second time by the page. The
// first ORDERBOOK_INIT message switches this instance into a dedicated depth
// data-plane that never starts the main Alpha stream or chart calculations.
// Profile snapshots use a double-buffered SharedArrayBuffer when cross-origin
// isolation is available, with a zero-copy transferable ArrayBuffer fallback.
// -----------------------------------------------------------------------------

const ORDER_BOOK_PROFILE_SCHEMA_VERSION = 2;
const ORDER_BOOK_SHARED_SCHEMA_VERSION = ORDER_BOOK_PROFILE_SCHEMA_VERSION;
const ORDER_BOOK_TRANSPORT_MODE = Object.freeze({
    SHARED: "shared",
    TRANSFER: "transfer"
});
const ORDER_BOOK_TRANSFER_BUFFER_LIMIT = 2;

const ORDER_BOOK_SHARED_HEADER = Object.freeze({
    MAGIC: 0,
    VERSION: 1,
    STATE: 2,
    SEQUENCE: 3,
    ACTIVE_SLOT: 4,
    SLOT0_COUNT: 5,
    SLOT1_COUNT: 6,
    CAPACITY: 7,
    STRIDE: 8,
    GENERATION: 9,
    ERROR_CODE: 10,
    HEARTBEAT: 11
});

const ORDER_BOOK_SHARED_STATE = Object.freeze({
    DISABLED: 0,
    STARTING: 1,
    SNAPSHOT_LOADING: 2,
    ONLINE: 3,
    ERROR: 4,
    STOPPED: 5
});

let orderBookProfileRuntime = null;

function postOrderBookMessage(type, payload = {}, transferList = undefined) {
    const message = { type, payload, workerVersion: WORKER_VERSION, workerBootId };
    if (Array.isArray(transferList) && transferList.length > 0) self.postMessage(message, transferList);
    else self.postMessage(message);
}

function sanitizeOrderBookTransportMode(value) {
    return value === ORDER_BOOK_TRANSPORT_MODE.SHARED
        ? ORDER_BOOK_TRANSPORT_MODE.SHARED
        : ORDER_BOOK_TRANSPORT_MODE.TRANSFER;
}

function getOrderBookSharedArrayBufferConstructor() {
    try {
        const runtimeGlobal = typeof globalThis === "object" && globalThis !== null ? globalThis : null;
        const constructor = runtimeGlobal ? runtimeGlobal["SharedArrayBuffer"] : undefined;
        return typeof constructor === "function" ? constructor : null;
    } catch {
        return null;
    }
}

function canUseOrderBookSharedTransport(sharedBuffer) {
    const runtimeGlobal = typeof globalThis === "object" && globalThis !== null ? globalThis : null;
    const SharedArrayBufferConstructor = getOrderBookSharedArrayBufferConstructor();
    const atomics = runtimeGlobal ? runtimeGlobal["Atomics"] : undefined;
    return SharedArrayBufferConstructor !== null
        && typeof atomics === "object"
        && atomics !== null
        && sharedBuffer instanceof SharedArrayBufferConstructor;
}

function sanitizeOrderBookWorkerSettings(rawSettings, marketOrToken = DEFAULT_MARKET) {
    const source = rawSettings && typeof rawSettings === "object" ? rawSettings : {};
    return {
        enabled: source.enabled !== false,
        levelsPerSide: clampInteger(source.levelsPerSide, 1, 500, 50),
        depthInterval: getDepthIntervalSetting(source.depthInterval, marketOrToken),
        publishIntervalMs: clampInteger(source.publishIntervalMs, 40, 1000, 100)
    };
}

function sanitizeOrderBookAnchor(rawAnchor, expectedRequestSymbol, expectedGeneration = null) {
    const source = rawAnchor && typeof rawAnchor === "object" ? rawAnchor : {};
    const price = Number(source.price);
    const eventTime = Number(source.eventTime);
    const requestSymbol = String(source.requestSymbol || "").trim().toUpperCase();
    const expectedSymbol = String(expectedRequestSymbol || "").trim().toUpperCase();
    const generation = Number(source.generation);

    if (!Number.isFinite(price) || price <= 0) return null;
    if (!Number.isFinite(eventTime) || eventTime <= 0) return null;
    if (!requestSymbol || (expectedSymbol && requestSymbol !== expectedSymbol)) return null;
    if (expectedGeneration !== null) {
        const requiredGeneration = Number(expectedGeneration);
        if (!Number.isFinite(generation) || generation !== requiredGeneration) return null;
    }
    return { price, eventTime, requestSymbol };
}

function sanitizeOrderBookViewport(rawViewport, expectedRequestSymbol, expectedGeneration = null) {
    const source = rawViewport && typeof rawViewport === "object" ? rawViewport : {};
    const visiblePriceMin = Number(source.visiblePriceMin);
    const visiblePriceMax = Number(source.visiblePriceMax);
    const bitmapHeight = Math.round(Number(source.bitmapHeight));
    const bucketBitmapPx = clampNumber(source.bucketBitmapPx, 1, 8, 1);
    const paddingRatio = clampNumber(source.paddingRatio, 0, 0.25, 0.08);
    const requestSymbol = String(source.requestSymbol || "").trim().toUpperCase();
    const expectedSymbol = String(expectedRequestSymbol || "").trim().toUpperCase();
    const generation = Number(source.generation);
    const version = Math.trunc(Number(source.version));

    if (!Number.isFinite(visiblePriceMin) || visiblePriceMin < 0) return null;
    if (!Number.isFinite(visiblePriceMax) || visiblePriceMax <= visiblePriceMin) return null;
    if (!Number.isFinite(bitmapHeight) || bitmapHeight < 16 || bitmapHeight > 32768) return null;
    if (!requestSymbol || (expectedSymbol && requestSymbol !== expectedSymbol)) return null;
    if (!Number.isFinite(version) || version <= 0) return null;
    if (expectedGeneration !== null) {
        const requiredGeneration = Number(expectedGeneration);
        if (!Number.isFinite(generation) || generation !== requiredGeneration) return null;
    }
    return {
        visiblePriceMin,
        visiblePriceMax,
        bitmapHeight,
        bucketBitmapPx,
        paddingRatio,
        requestSymbol,
        version
    };
}

function validateOrderBookLayout(rawLayout, sharedBuffer = null, transportMode = ORDER_BOOK_TRANSPORT_MODE.TRANSFER) {
    const layout = rawLayout && typeof rawLayout === "object" ? rawLayout : {};
    const headerInts = clampInteger(layout.headerInts, 16, 256, 64);
    const capacity = clampInteger(layout.capacity, 64, 4096, 1024);
    const stride = clampInteger(layout.stride, 4, 16, 8);
    const slotMetaDoubles = clampInteger(layout.slotMetaDoubles, 12, 32, 12);
    const headerBytes = headerInts * Int32Array.BYTES_PER_ELEMENT;
    const slotDoubles = slotMetaDoubles + capacity * stride;
    const slotBytes = slotDoubles * Float64Array.BYTES_PER_ELEMENT;
    const byteLength = headerBytes + slotBytes * 2;
    if (transportMode === ORDER_BOOK_TRANSPORT_MODE.SHARED) {
        if (!canUseOrderBookSharedTransport(sharedBuffer)) throw new Error("ORDERBOOK_INIT shared transport requires SharedArrayBuffer");
        if (sharedBuffer.byteLength < byteLength) {
            throw new Error(`SharedArrayBuffer too small: ${sharedBuffer.byteLength} < ${byteLength}`);
        }
    }
    return { headerInts, capacity, stride, slotMetaDoubles, headerBytes, slotDoubles, slotBytes, byteLength };
}

function getDepthStreamName(token, rawInterval) {
    const market = getTokenMarket(token);
    const streamSymbol = getStreamSymbol(token);
    const interval = getDepthIntervalSetting(rawInterval, market);
    if (market === MARKET.ALPHA) return `${streamSymbol}@fulldepth@${interval}`;
    if (market === MARKET.SPOT) return interval === "100ms" ? `${streamSymbol}@depth@100ms` : `${streamSymbol}@depth`;
    return `${streamSymbol}@depth@${interval}`;
}

function getDepthSnapshotUrl(token) {
    const config = getMarketConfig(token);
    const symbol = encodeURIComponent(getRequestSymbol(token));
    return `${config.depthUrl}?symbol=${symbol}&limit=${config.depthLimit}`;
}

function normalizeDepthSnapshotPayload(payload, token) {
    const market = getTokenMarket(token);
    if (market === MARKET.ALPHA) {
        if (!payload?.success || !payload?.data) throw new Error(payload?.message || "Invalid Alpha fullDepth response");
        if (payload.data.symbol && payload.data.symbol !== getRequestSymbol(token)) throw new Error("Snapshot symbol mismatch");
        return payload.data;
    }
    if (!payload || !Array.isArray(payload.bids) || !Array.isArray(payload.asks) || !Number.isFinite(Number(payload.lastUpdateId))) {
        throw new Error(`Invalid ${getMarketLabel(market)} depth response`);
    }
    return payload;
}

class OrderBookProfileRuntime {
    constructor(payload) {
        this.transportMode = sanitizeOrderBookTransportMode(payload.transportMode);
        this.sharedBuffer = this.transportMode === ORDER_BOOK_TRANSPORT_MODE.SHARED ? payload.sharedBuffer : null;
        this.layout = validateOrderBookLayout(payload.layout, this.sharedBuffer, this.transportMode);
        this.header = this.transportMode === ORDER_BOOK_TRANSPORT_MODE.SHARED
            ? new Int32Array(this.sharedBuffer, 0, this.layout.headerInts)
            : null;
        this.generation = Number(payload.generation) || 0;
        this.token = payload.token || null;
        this.market = getTokenMarket(this.token);
        this.settings = sanitizeOrderBookWorkerSettings(payload.settings, this.market);
        this.book = new OrderBook(this.market);
        this.ws = null;
        this.closed = true;
        this.connectionGeneration = 0;
        this.socketDescriptor = null;
        this.reconnectAttempt = 0;
        this.reconnectTimer = null;
        this.snapshotAbort = null;
        this.snapshotReady = false;
        this.depthBuffer = [];
        this.publishTimer = null;
        this.lastPublishAt = 0;
        this.pendingPublish = false;
        this.status = "created";
        this.lastError = "";
        this.anchorPrice = 0;
        this.anchorEventTime = 0;
        this.anchorReceivedAt = 0;
        this.viewport = null;
        this.viewportVersion = 0;
        this.runtimeState = ORDER_BOOK_SHARED_STATE.DISABLED;
        this.transferSequence = 0;
        this.transferBufferPool = [];
        this.transferBuffersCreated = 0;
        this.transferBuffersInFlight = 0;
        const initialAnchor = sanitizeOrderBookAnchor(
            payload.anchor,
            getRequestSymbol(this.token),
            this.generation
        );
        if (initialAnchor) {
            this.anchorPrice = initialAnchor.price;
            this.anchorEventTime = initialAnchor.eventTime;
            this.anchorReceivedAt = nowMs();
        }

        if (this.header) {
            Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.MAGIC, 0x41535042);
            Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.VERSION, ORDER_BOOK_SHARED_SCHEMA_VERSION);
            Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.CAPACITY, this.layout.capacity);
            Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.STRIDE, this.layout.stride);
            Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.GENERATION, this.generation);
            Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.ERROR_CODE, 0);
        }
    }

    getMessageContext() {
        return {
            generation: this.generation,
            market: this.market,
            requestSymbol: getRequestSymbol(this.token),
            tokenKey: getTokenKey(this.token),
            transportMode: this.transportMode
        };
    }

    setRuntimeState(code, status, text, error = "") {
        this.runtimeState = code;
        this.status = status;
        this.lastError = error || "";
        if (this.header) {
            Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.STATE, code);
            Atomics.add(this.header, ORDER_BOOK_SHARED_HEADER.HEARTBEAT, 1);
            Atomics.notify(this.header, ORDER_BOOK_SHARED_HEADER.STATE);
        }
        postOrderBookMessage("ORDERBOOK_STATUS", {
            ...this.getMessageContext(),
            state: code,
            status,
            text,
            error
        });
    }

    start() {
        if (!this.settings.enabled) {
            this.stop("disabled");
            return;
        }
        if (!this.token || !getRequestSymbol(this.token) || !getStreamSymbol(this.token)) {
            throw new Error("Order-book worker token is invalid");
        }
        this.closed = false;
        this.reconnectAttempt = 0;
        this.setRuntimeState(ORDER_BOOK_SHARED_STATE.STARTING, "starting", "Запуск автономного depth worker");
        postOrderBookMessage("ORDERBOOK_READY", { ...this.getMessageContext(), text: `Worker ${WORKER_VERSION} · ${getRequestSymbol(this.token)}` });
        this.connect();
    }

    stop(reason = "stopped") {
        this.closed = true;
        this.connectionGeneration += 1;
        if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
        if (this.publishTimer !== null) clearTimeout(this.publishTimer);
        this.reconnectTimer = null;
        this.publishTimer = null;
        this.pendingPublish = false;
        if (this.snapshotAbort) this.snapshotAbort.abort();
        this.snapshotAbort = null;
        this.closeSocket(1000, reason, true);
        this.depthBuffer = [];
        this.snapshotReady = false;
        this.anchorPrice = 0;
        this.anchorEventTime = 0;
        this.anchorReceivedAt = 0;
        this.viewport = null;
        this.viewportVersion = 0;
        this.book.reset();
        this.runtimeState = ORDER_BOOK_SHARED_STATE.STOPPED;
        if (this.header) {
            Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.SLOT0_COUNT, 0);
            Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.SLOT1_COUNT, 0);
            Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.STATE, ORDER_BOOK_SHARED_STATE.STOPPED);
            Atomics.add(this.header, ORDER_BOOK_SHARED_HEADER.HEARTBEAT, 1);
            Atomics.notify(this.header, ORDER_BOOK_SHARED_HEADER.STATE);
        }
        postOrderBookMessage("ORDERBOOK_STATUS", {
            ...this.getMessageContext(),
            state: ORDER_BOOK_SHARED_STATE.STOPPED,
            status: "stopped",
            text: `Стакан зупинено · ${reason}`
        });
    }

    closeSocket(code, reason, detachHandlers = false) {
        const ws = this.ws;
        this.ws = null;
        if (!ws) return;
        if (detachHandlers) {
            ws.onopen = null;
            ws.onmessage = null;
            ws.onerror = null;
            ws.onclose = null;
        }
        if (ws.readyState === WebSocket.CLOSING || ws.readyState === WebSocket.CLOSED) return;
        try { ws.close(code, String(reason || "closed").slice(0, 120)); } catch { /* no-op */ }
    }

    isCurrentConnection(ws, generation) {
        return !this.closed && this.ws === ws && this.connectionGeneration === generation;
    }

    connect() {
        if (this.closed) return;
        if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
        this.closeSocket(1000, "reconnect", true);
        this.snapshotReady = false;
        this.depthBuffer = [];
        const depthStream = getDepthStreamName(this.token, this.settings.depthInterval);
        const descriptor = createWebSocketStreamDescriptor(this.token, depthStream, {
            route: FUTURES_WEBSOCKET_ROUTE.PUBLIC
        });
        const generation = this.connectionGeneration + 1;
        let ws;
        try {
            ws = new WebSocket(descriptor.url);
        } catch (error) {
            this.failAndReconnect("WebSocket init", error);
            return;
        }
        this.connectionGeneration = generation;
        this.socketDescriptor = descriptor;
        this.ws = ws;

        ws.onopen = () => {
            if (!this.isCurrentConnection(ws, generation)) return;
            this.reconnectAttempt = 0;
            if (descriptor.mode === "subscribe") {
                try {
                    ws.send(JSON.stringify({ method: "SUBSCRIBE", params: [descriptor.streamName], id: Date.now() }));
                } catch (error) {
                    this.failAndReconnect("Depth subscribe", error);
                    return;
                }
            }
            this.setRuntimeState(ORDER_BOOK_SHARED_STATE.SNAPSHOT_LOADING, "snapshot", `Depth stream відкрито · ${this.settings.depthInterval}`);
            this.loadSnapshotWithRetry(generation);
        };
        ws.onmessage = event => {
            if (!this.isCurrentConnection(ws, generation)) return;
            this.handleSocketMessage(event);
        };
        ws.onerror = () => {
            if (!this.isCurrentConnection(ws, generation)) return;
            this.setRuntimeState(ORDER_BOOK_SHARED_STATE.ERROR, "error", "WebSocket помилка стакана", "websocket_error");
        };
        ws.onclose = event => {
            if (!this.isCurrentConnection(ws, generation)) return;
            this.ws = null;
            if (this.closed) return;
            this.scheduleReconnect(`WebSocket closed ${event.code}`);
        };
    }

    scheduleReconnect(reason) {
        if (this.closed || this.reconnectTimer !== null) return;
        this.reconnectAttempt += 1;
        const base = Math.max(250, Number(CONFIG.reconnectBaseMs) || 900);
        const max = Math.max(base, Number(CONFIG.reconnectMaxMs) || 30000);
        const delay = Math.min(max, base * (2 ** Math.min(8, this.reconnectAttempt - 1))) + Math.floor(Math.random() * 250);
        this.setRuntimeState(ORDER_BOOK_SHARED_STATE.STARTING, "reconnecting", `${reason} · retry ${delay}ms`);
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            this.connect();
        }, delay);
    }

    failAndReconnect(context, error) {
        const message = error?.message || String(error || "unknown error");
        this.lastError = `${context}: ${message}`;
        if (this.header) Atomics.add(this.header, ORDER_BOOK_SHARED_HEADER.ERROR_CODE, 1);
        this.setRuntimeState(ORDER_BOOK_SHARED_STATE.ERROR, "error", context, message);
        postOrderBookMessage("ORDERBOOK_ERROR", { ...this.getMessageContext(), context, message });
        this.scheduleReconnect(context);
    }

    async loadSnapshotWithRetry(generation) {
        for (let attempt = 1; attempt <= CONFIG.snapshotRetryMax; attempt += 1) {
            if (this.closed || generation !== this.connectionGeneration) return;
            try {
                await this.loadSnapshot(generation);
                return;
            } catch (error) {
                if (this.closed || generation !== this.connectionGeneration) return;
                if (attempt >= CONFIG.snapshotRetryMax) {
                    this.failAndReconnect(`${getMarketLabel(this.market)} depth snapshot`, error);
                    return;
                }
                await sleep(400 * attempt);
            }
        }
    }

    async loadSnapshot(generation) {
        if (this.snapshotAbort) this.snapshotAbort.abort();
        const controller = new AbortController();
        this.snapshotAbort = controller;
        const requestSymbol = getRequestSymbol(this.token);
        const payload = await fetchJsonWithTimeout(getDepthSnapshotUrl(this.token), { signal: controller.signal }, 12000);
        if (this.snapshotAbort === controller) this.snapshotAbort = null;
        if (this.closed || generation !== this.connectionGeneration) return;
        const snapshot = normalizeDepthSnapshotPayload(payload, this.token);
        this.book.loadSnapshot(snapshot);
        this.snapshotReady = true;
        if (!this.replayBufferedDepth()) return;
        this.setRuntimeState(ORDER_BOOK_SHARED_STATE.ONLINE, "online", `Онлайн · ${requestSymbol} · ${this.settings.levelsPerSide}×2 рівнів`);
        this.publishProfile(true);
    }

    replayBufferedDepth() {
        if (!this.depthBuffer.length) return true;
        const buffered = this.depthBuffer.splice(0).sort((a, b) => (Number(a.U || a.u) || 0) - (Number(b.U || b.u) || 0));
        for (const delta of buffered) {
            const result = this.book.applyDelta(delta);
            if (result.applied || result.reason === "old_update") continue;
            if (result.reason === "sequence_gap" || result.reason === "possible_gap") {
                this.resync(`buffered_${result.reason}`);
                return false;
            }
        }
        return true;
    }

    resync(reason) {
        if (this.closed) return;
        this.snapshotReady = false;
        this.depthBuffer = [];
        this.book.reset();
        this.setRuntimeState(ORDER_BOOK_SHARED_STATE.SNAPSHOT_LOADING, "resync", `Ресинхронізація · ${reason}`);
        this.loadSnapshotWithRetry(this.connectionGeneration);
    }

    handleSocketMessage(event) {
        let message;
        try { message = JSON.parse(event.data); } catch { return; }
        if (message && Object.prototype.hasOwnProperty.call(message, "result")) return;
        if (message && Object.prototype.hasOwnProperty.call(message, "code")) {
            this.failAndReconnect("Depth subscription rejected", new Error(message.msg || `code ${message.code}`));
            return;
        }
        const data = message?.data || message;
        if (!data || typeof data !== "object") return;
        const expectedSymbol = getRequestSymbol(this.token);
        if (data.s && data.s !== expectedSymbol) return;

        if (data.e !== "depthUpdate") return;
        if (!this.snapshotReady) {
            this.depthBuffer.push(data);
            if (this.depthBuffer.length > 2000) this.depthBuffer.shift();
            return;
        }
        const result = this.book.applyDelta(data);
        if (!result.applied) {
            if (result.reason === "old_update") return;
            if (result.reason === "sequence_gap" || result.reason === "possible_gap") this.resync(result.reason);
            return;
        }
        this.schedulePublish();
    }

    schedulePublish() {
        if (this.closed || !this.snapshotReady) return;
        this.pendingPublish = true;
        if (this.publishTimer !== null) return;
        const elapsed = nowMs() - this.lastPublishAt;
        const delay = Math.max(0, this.settings.publishIntervalMs - elapsed);
        this.publishTimer = setTimeout(() => {
            this.publishTimer = null;
            if (!this.pendingPublish) return;
            this.pendingPublish = false;
            this.publishProfile(false);
        }, delay);
    }

    getGroupPrecision() {
        const raw = Number(this.token?.pricePrecision ?? this.token?.tradeDecimal ?? 8);
        if (!Number.isFinite(raw)) return 8;
        return Math.max(0, Math.min(10, Math.floor(raw)));
    }

    addLevelToGroup(grouped, level, side, precision) {
        if (!level || !Number.isFinite(level.price) || level.price <= 0 || !Number.isFinite(level.qty) || level.qty <= 0) return;
        const key = level.price.toFixed(precision);
        const price = Number(key);
        if (!Number.isFinite(price) || price <= 0) return;
        let group = grouped.get(key);
        if (!group) {
            group = { price, qty: 0, notional: 0, bidNotional: 0, askNotional: 0, minPrice: level.price, maxPrice: level.price };
            grouped.set(key, group);
        }
        const notional = Number.isFinite(level.notional) && level.notional > 0 ? level.notional : level.price * level.qty;
        group.qty += level.qty;
        group.notional += notional;
        if (side === "bid") group.bidNotional += notional;
        else group.askNotional += notional;
        group.minPrice = Math.min(group.minPrice, level.price);
        group.maxPrice = Math.max(group.maxPrice, level.price);
    }

    updateAnchor(rawAnchor) {
        if (this.closed) return false;
        const anchor = sanitizeOrderBookAnchor(
            rawAnchor,
            getRequestSymbol(this.token),
            this.generation
        );
        if (!anchor || anchor.eventTime < this.anchorEventTime) return false;

        const changed = anchor.price !== this.anchorPrice || anchor.eventTime !== this.anchorEventTime;
        this.anchorPrice = anchor.price;
        this.anchorEventTime = anchor.eventTime;
        this.anchorReceivedAt = nowMs();
        if (changed) this.schedulePublish();
        return true;
    }

    getProfileAnchorPrice() {
        const anchorMaxAgeMs = Math.max(1000, Number(CONFIG.quoteRecoveryAnchorMaxAgeMs) || 4000);
        const anchorAgeMs = this.anchorReceivedAt > 0 ? nowMs() - this.anchorReceivedAt : Number.POSITIVE_INFINITY;
        if (
            Number.isFinite(this.anchorPrice)
            && this.anchorPrice > 0
            && anchorAgeMs >= 0
            && anchorAgeMs <= anchorMaxAgeMs
        ) {
            return this.anchorPrice;
        }
        const bid = Number(this.book.bidLevels[0]?.price);
        const ask = Number(this.book.askLevels[0]?.price);
        const hasBid = Number.isFinite(bid) && bid > 0;
        const hasAsk = Number.isFinite(ask) && ask > 0;
        if (hasBid && hasAsk) return (bid + ask) / 2;
        return hasBid ? bid : (hasAsk ? ask : null);
    }

    collectProfileSideLevels(levels, side, anchorPrice, limit) {
        const accepted = [];
        const isBid = side === "bid";
        for (const level of levels) {
            const price = Number(level?.price);
            if (!Number.isFinite(price) || price <= 0) continue;
            if (anchorPrice && (isBid ? price > anchorPrice : price < anchorPrice)) continue;
            accepted.push(level);
            if (accepted.length >= limit) break;
        }
        return accepted;
    }

    updateViewport(rawViewport) {
        if (this.closed) return false;
        const viewport = sanitizeOrderBookViewport(
            rawViewport,
            getRequestSymbol(this.token),
            this.generation
        );
        if (!viewport || viewport.version <= this.viewportVersion) return false;

        this.viewport = viewport;
        this.viewportVersion = viewport.version;
        this.schedulePublish();
        return true;
    }

    addLevelToViewportBucket(grouped, level, side, viewportPlan) {
        if (!level || !Number.isFinite(level.price) || level.price <= 0 || !Number.isFinite(level.qty) || level.qty <= 0) return;
        if (level.price < viewportPlan.paddedPriceMin || level.price > viewportPlan.paddedPriceMax) return;

        const normalizedBucket = (level.price - viewportPlan.bucketOrigin) / viewportPlan.priceBucketSize;
        const boundaryTolerance = Math.max(1, Math.abs(normalizedBucket)) * Number.EPSILON * 8;
        const bucketIndex = Math.floor(normalizedBucket + boundaryTolerance);
        if (!Number.isFinite(bucketIndex) || bucketIndex < 0 || bucketIndex >= viewportPlan.bucketCount) return;
        let group = grouped.get(bucketIndex);
        if (!group) {
            group = {
                price: 0,
                qty: 0,
                notional: 0,
                bidNotional: 0,
                askNotional: 0,
                minPrice: level.price,
                maxPrice: level.price,
                weightedPriceNotional: 0
            };
            grouped.set(bucketIndex, group);
        }

        const notional = Number.isFinite(level.notional) && level.notional > 0
            ? level.notional
            : level.price * level.qty;
        if (!Number.isFinite(notional) || notional <= 0) return;
        group.qty += level.qty;
        group.notional += notional;
        group.weightedPriceNotional += level.price * notional;
        if (side === "bid") group.bidNotional += notional;
        else group.askNotional += notional;
        group.minPrice = Math.min(group.minPrice, level.price);
        group.maxPrice = Math.max(group.maxPrice, level.price);
    }

    createViewportCompactionPlan() {
        const viewport = this.viewport;
        if (!viewport) return null;
        const priceRange = viewport.visiblePriceMax - viewport.visiblePriceMin;
        if (!Number.isFinite(priceRange) || priceRange <= 0 || viewport.bitmapHeight < 16) return null;

        const priceBucketSize = priceRange / viewport.bitmapHeight * viewport.bucketBitmapPx;
        if (!Number.isFinite(priceBucketSize) || priceBucketSize <= 0) return null;
        const paddingPrice = priceRange * viewport.paddingRatio;
        const paddedPriceMin = Math.max(0, viewport.visiblePriceMin - paddingPrice);
        const paddedPriceMax = viewport.visiblePriceMax + paddingPrice;
        const bucketOrigin = paddedPriceMin;
        const bucketCount = Math.max(1, Math.ceil((paddedPriceMax - paddedPriceMin) / priceBucketSize) + 1);
        if (!Number.isFinite(bucketOrigin) || !Number.isFinite(bucketCount) || bucketCount > 131072) return null;

        return {
            paddedPriceMin,
            paddedPriceMax,
            bucketOrigin,
            bucketCount,
            priceBucketSize,
            viewportVersion: viewport.version
        };
    }

    buildViewportCompactedLevels(bids, asks, viewportPlan) {
        const grouped = new Map();
        for (const level of bids) this.addLevelToViewportBucket(grouped, level, "bid", viewportPlan);
        for (const level of asks) this.addLevelToViewportBucket(grouped, level, "ask", viewportPlan);

        const levels = [];
        for (const group of grouped.values()) {
            if (!Number.isFinite(group.notional) || group.notional <= 0) continue;
            group.price = group.weightedPriceNotional > 0
                ? group.weightedPriceNotional / group.notional
                : (group.minPrice + group.maxPrice) / 2;
            levels.push(group);
        }
        levels.sort((a, b) => a.price - b.price);
        return levels;
    }

    buildProfileLevels() {
        const precision = this.getGroupPrecision();
        const limit = this.settings.levelsPerSide;
        const anchorPrice = this.getProfileAnchorPrice();
        const bids = this.collectProfileSideLevels(this.book.bidLevels, "bid", anchorPrice, limit);
        const asks = this.collectProfileSideLevels(this.book.askLevels, "ask", anchorPrice, limit);
        const viewportPlan = this.createViewportCompactionPlan();
        let levels;

        if (viewportPlan) {
            levels = this.buildViewportCompactedLevels(bids, asks, viewportPlan);
        } else {
            const grouped = new Map();
            for (const level of bids) this.addLevelToGroup(grouped, level, "bid", precision);
            for (const level of asks) this.addLevelToGroup(grouped, level, "ask", precision);
            levels = Array.from(grouped.values()).sort((a, b) => a.price - b.price);
        }

        if (levels.length > this.layout.capacity) levels.length = this.layout.capacity;
        let maxNotional = 0;
        for (const level of levels) maxNotional = Math.max(maxNotional, level.notional);
        return {
            levels,
            precision,
            maxNotional,
            compacted: viewportPlan !== null,
            priceBucketSize: viewportPlan?.priceBucketSize || 0,
            sourceLevelCount: bids.length + asks.length,
            viewportVersion: viewportPlan?.viewportVersion || 0
        };
    }

    acquireTransferBuffer() {
        while (this.transferBufferPool.length > 0) {
            const candidate = this.transferBufferPool.pop();
            if (candidate instanceof ArrayBuffer && candidate.byteLength === this.layout.slotBytes) return candidate;
        }
        if (this.transferBuffersCreated >= ORDER_BOOK_TRANSFER_BUFFER_LIMIT) return null;
        this.transferBuffersCreated += 1;
        return new ArrayBuffer(this.layout.slotBytes);
    }

    recycleTransferBuffer(rawBuffer, rawGeneration, rawRequestSymbol) {
        if (this.transportMode !== ORDER_BOOK_TRANSPORT_MODE.TRANSFER) return false;
        const generation = Number(rawGeneration);
        const requestSymbol = String(rawRequestSymbol || "").trim().toUpperCase();
        if (!Number.isFinite(generation) || generation !== this.generation) return false;
        if (requestSymbol !== getRequestSymbol(this.token)) return false;
        if (!(rawBuffer instanceof ArrayBuffer) || rawBuffer.byteLength !== this.layout.slotBytes) return false;
        if (this.transferBuffersInFlight > 0) this.transferBuffersInFlight -= 1;
        if (this.transferBufferPool.length >= ORDER_BOOK_TRANSFER_BUFFER_LIMIT) return false;
        this.transferBufferPool.push(rawBuffer);
        if (this.pendingPublish && this.publishTimer === null && !this.closed) this.schedulePublish();
        return true;
    }

    writeProfileSlot(slotView, built, now) {
        const bestBid = this.book.bidLevels[0]?.price || 0;
        const bestAsk = this.book.askLevels[0]?.price || 0;
        slotView[0] = Number(this.book.lastUpdateId) || 0;
        slotView[1] = Number(this.book.lastEventTime) || now;
        slotView[2] = now;
        slotView[3] = built.maxNotional;
        slotView[4] = bestBid;
        slotView[5] = bestAsk;
        slotView[6] = Number(this.book.changeSeq) || 0;
        slotView[7] = built.precision;
        if (this.layout.slotMetaDoubles > 8) slotView[8] = built.compacted ? 1 : 0;
        if (this.layout.slotMetaDoubles > 9) slotView[9] = built.priceBucketSize;
        if (this.layout.slotMetaDoubles > 10) slotView[10] = built.sourceLevelCount;
        if (this.layout.slotMetaDoubles > 11) slotView[11] = built.viewportVersion;

        const stride = this.layout.stride;
        let offset = this.layout.slotMetaDoubles;
        for (const level of built.levels) {
            slotView[offset] = level.price;
            slotView[offset + 1] = level.qty;
            slotView[offset + 2] = level.notional;
            slotView[offset + 3] = level.bidNotional;
            slotView[offset + 4] = level.askNotional;
            slotView[offset + 5] = level.bidNotional > 0 && level.askNotional > 0 ? 3 : (level.bidNotional > 0 ? 1 : 2);
            slotView[offset + 6] = level.minPrice;
            slotView[offset + 7] = level.maxPrice;
            offset += stride;
        }
    }

    publishSharedProfile(built, now) {
        if (!this.header || !this.sharedBuffer) return false;
        const activeSlot = Atomics.load(this.header, ORDER_BOOK_SHARED_HEADER.ACTIVE_SLOT) & 1;
        const writeSlot = activeSlot === 0 ? 1 : 0;
        const countIndex = writeSlot === 0 ? ORDER_BOOK_SHARED_HEADER.SLOT0_COUNT : ORDER_BOOK_SHARED_HEADER.SLOT1_COUNT;
        const slotOffset = this.layout.headerBytes + writeSlot * this.layout.slotBytes;
        const slotView = new Float64Array(this.sharedBuffer, slotOffset, this.layout.slotDoubles);
        this.writeProfileSlot(slotView, built, now);

        Atomics.store(this.header, countIndex, built.levels.length);
        Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.ACTIVE_SLOT, writeSlot);
        const sequence = Atomics.add(this.header, ORDER_BOOK_SHARED_HEADER.SEQUENCE, 1) + 1;
        Atomics.store(this.header, ORDER_BOOK_SHARED_HEADER.STATE, ORDER_BOOK_SHARED_STATE.ONLINE);
        Atomics.add(this.header, ORDER_BOOK_SHARED_HEADER.HEARTBEAT, 1);
        Atomics.notify(this.header, ORDER_BOOK_SHARED_HEADER.SEQUENCE);
        this.runtimeState = ORDER_BOOK_SHARED_STATE.ONLINE;
        return sequence > 0;
    }

    publishTransferProfile(built, now) {
        const buffer = this.acquireTransferBuffer();
        if (!buffer) {
            this.pendingPublish = true;
            return false;
        }
        const slotView = new Float64Array(buffer);
        this.writeProfileSlot(slotView, built, now);
        const sequence = this.transferSequence + 1;
        const payload = {
            ...this.getMessageContext(),
            schemaVersion: ORDER_BOOK_PROFILE_SCHEMA_VERSION,
            sequence,
            count: built.levels.length,
            stride: this.layout.stride,
            slotMetaDoubles: this.layout.slotMetaDoubles,
            buffer
        };
        try {
            postOrderBookMessage("ORDERBOOK_PROFILE_TRANSFER", payload, [buffer]);
            this.transferBuffersInFlight += 1;
        } catch (error) {
            if (buffer.byteLength === this.layout.slotBytes && this.transferBufferPool.length < ORDER_BOOK_TRANSFER_BUFFER_LIMIT) {
                this.transferBufferPool.push(buffer);
            }
            this.lastError = `transfer publish: ${error?.message || String(error)}`;
            postOrderBookMessage("ORDERBOOK_ERROR", {
                ...this.getMessageContext(),
                context: "transfer_publish",
                message: error?.message || String(error)
            });
            return false;
        }
        this.transferSequence = sequence;
        this.runtimeState = ORDER_BOOK_SHARED_STATE.ONLINE;
        return true;
    }

    publishProfile(force) {
        if (this.closed || !this.snapshotReady || !this.book.snapshotLoaded) return false;
        const now = nowMs();
        if (!force && now - this.lastPublishAt < this.settings.publishIntervalMs) {
            this.schedulePublish();
            return false;
        }
        const built = this.buildProfileLevels();
        const published = this.transportMode === ORDER_BOOK_TRANSPORT_MODE.SHARED
            ? this.publishSharedProfile(built, now)
            : this.publishTransferProfile(built, now);
        if (published) {
            this.lastPublishAt = now;
            this.pendingPublish = false;
        }
        return published;
    }
}

function handleOrderBookWorkerMessage(event) {
    const message = event?.data || {};
    const payload = message.payload || {};
    try {
        switch (message.type) {
            case "ORDERBOOK_INIT": {
                if (orderBookProfileRuntime) orderBookProfileRuntime.stop("reinitialize");
                orderBookProfileRuntime = new OrderBookProfileRuntime(payload);
                orderBookProfileRuntime.start();
                return;
            }
            case "ORDERBOOK_STOP": {
                if (orderBookProfileRuntime) orderBookProfileRuntime.stop(payload.reason || "main_stop");
                orderBookProfileRuntime = null;
                return;
            }
            case "ORDERBOOK_ANCHOR_UPDATE": {
                if (!orderBookProfileRuntime) return;
                orderBookProfileRuntime.updateAnchor(payload);
                return;
            }
            case "ORDERBOOK_VIEWPORT_UPDATE": {
                if (!orderBookProfileRuntime) return;
                orderBookProfileRuntime.updateViewport(payload);
                return;
            }
            case "ORDERBOOK_RECYCLE_TRANSFER_BUFFER": {
                if (!orderBookProfileRuntime) return;
                orderBookProfileRuntime.recycleTransferBuffer(
                    payload.buffer,
                    payload.generation,
                    payload.requestSymbol
                );
                return;
            }
            case "ORDERBOOK_UPDATE": {
                if (!orderBookProfileRuntime) return;
                const previous = orderBookProfileRuntime.settings;
                const next = sanitizeOrderBookWorkerSettings(payload.settings, orderBookProfileRuntime.market);
                const restart = previous.depthInterval !== next.depthInterval;
                orderBookProfileRuntime.settings = next;
                if (!next.enabled) {
                    orderBookProfileRuntime.stop("disabled");
                    orderBookProfileRuntime = null;
                } else if (restart) {
                    orderBookProfileRuntime.stop("interval_changed");
                    orderBookProfileRuntime.closed = false;
                    orderBookProfileRuntime.start();
                } else {
                    orderBookProfileRuntime.publishProfile(true);
                }
                return;
            }
            case "ORDERBOOK_PING": {
                const runtime = orderBookProfileRuntime;
                postOrderBookMessage("ORDERBOOK_STATUS", {
                    ...(runtime ? runtime.getMessageContext() : {}),
                    state: runtime?.runtimeState ?? ORDER_BOOK_SHARED_STATE.DISABLED,
                    status: runtime?.status || "disabled",
                    text: runtime ? "pong" : "disabled"
                });
                return;
            }
            default:
                throw new Error(`Unknown order-book worker message type: ${message.type}`);
        }
    } catch (error) {
        if (orderBookProfileRuntime?.header) {
            Atomics.store(orderBookProfileRuntime.header, ORDER_BOOK_SHARED_HEADER.STATE, ORDER_BOOK_SHARED_STATE.ERROR);
            Atomics.add(orderBookProfileRuntime.header, ORDER_BOOK_SHARED_HEADER.ERROR_CODE, 1);
            Atomics.notify(orderBookProfileRuntime.header, ORDER_BOOK_SHARED_HEADER.STATE);
        }
        postOrderBookMessage("ORDERBOOK_ERROR", {
            ...(orderBookProfileRuntime ? orderBookProfileRuntime.getMessageContext() : {}),
            context: message.type || "message",
            message: error?.message || String(error)
        });
    }
}
