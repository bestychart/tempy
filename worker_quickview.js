// [QuickView Worker] v16.1 (Parametric Indicators + VWAP + Incremental + Validation)

// === CONFIGURATION ===
const NEW_COIN_PERIOD_MS = 8 * 7 * 24 * 60 * 60 * 1000; 
const MID_COIN_PERIOD_MS = 365 * 24 * 60 * 60 * 1000;   
const MAX_BUFFER_SIZE = 1500;
const CALC_LOOKBACK = 270; 

const ENDPOINTS = {
    ALPHA_LIST: "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list",
    ALPHA_KLINES: "https://www.binance.com/bapi/defi/v1/public/alpha-trade/klines",
    FUTURES_INFO: "https://fapi.binance.com/fapi/v1/exchangeInfo",
    FUTURES_KLINES: "https://fapi.binance.com/fapi/v1/klines",
    SPOT_INFO: "https://api.binance.com/api/v3/exchangeInfo",
    SPOT_KLINES: "https://api.binance.com/api/v3/klines"
};

let marketLists = { ALPHA: [], FUTURES: [], SPOT: [] };

// Динамічна структура для активних завдань (ініціалізується при INIT з chartCount)
let activeSubs = {}; 

let isPolling = false;
let areMarketsLoaded = false;

// === INDICATOR CONFIG (Параметри з UI, оновлюються через UPDATE_INDICATOR_CONFIG) ===
let indicatorConfig = {
    sma: { length: 7 },
    bb: { length: 20, multiplier: 2 },
    hma25: { length: 25 },
    hma35: { length: 35 },
    hma55: { length: 55 },
    hma100: { length: 100 },
    devCloud: { emaLength: 132, basisMode: 'ema' }
};

// Динамічний стан для інкрементального оновлення (ініціалізується при INIT)
let incrementalState = {};

// Кількість графіків (приходить від UI через INIT)
let chartCount = 2;

// Хелпер: створює порожній запис activeSub для заданого chartId
function createEmptySub() {
    return { market: null, symbol: null, timeframe: null, controller: null, isLoading: false, buffer: [], historyMode: false };
}

// Хелпер: ініціалізує activeSubs та incrementalState для N графіків
function initChartSlots(count) {
    chartCount = count;
    activeSubs = {};
    incrementalState = {};
    for (let i = 1; i <= count; i++) {
        activeSubs[i] = createEmptySub();
        incrementalState[i] = null;
    }
}

// === NETWORK HELPER ===
// 🔥 FIX: Додана підтримка зовнішнього signal для скасування
const fetchWithTimeout = async (url, options = {}, timeout = 8000) => {
    const controller = new AbortController();
    // Якщо передано зовнішній сигнал, слухаємо його теж
    if (options.signal) {
        options.signal.addEventListener('abort', () => controller.abort());
    }
    
    const id = setTimeout(() => controller.abort(), timeout);
    try {
        const response = await fetch(url, { ...options, signal: controller.signal });
        clearTimeout(id);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response;
    } catch (error) {
        clearTimeout(id);
        throw error;
    }
};

const getKlinesUrl = (market, symbol, timeframe, limit, alphaId = null, startTime = null, endTime = null) => {
    let url;
    if (market === 'ALPHA') {
        const reqSym = alphaId ? `${alphaId}USDT` : `${symbol}USDT`;
        url = `${ENDPOINTS.ALPHA_KLINES}?symbol=${reqSym}&interval=${timeframe}&limit=${limit}`;
    } else if (market === 'SPOT') {
        url = `${ENDPOINTS.SPOT_KLINES}?symbol=${symbol}&interval=${timeframe}&limit=${limit}`;
    } else {
        url = `${ENDPOINTS.FUTURES_KLINES}?symbol=${symbol}&interval=${timeframe}&limit=${limit}`;
    }
    if (startTime) url += `&startTime=${startTime}`;
    if (endTime) url += `&endTime=${endTime}`;
    return url;
};

// === MATH HELPERS (Ті самі, без змін) ===
// 1. Універсальна функція WMA, яка напряму мутує об'єкти (O(1) складність після старту)
function applyWMA(candles, period, sourceKey, targetKey) {
    const n = candles.length;
    if (n < period) return;

    const weightSum = (period * (period + 1)) / 2;
    let currentTotalSum = 0;
    let currentWeightedSum = 0;

    // Шукаємо першу свічку, де є базові дані (для HMA це може бути не 0-й індекс)
    let validStartIndex = -1;
    for (let i = 0; i < n; i++) {
        if (candles[i][sourceKey] !== undefined && candles[i][sourceKey] !== null) {
            validStartIndex = i;
            break;
        }
    }

    if (validStartIndex === -1 || n - validStartIndex < period) return;

    // Розрахунок першого вікна
    for (let i = 0; i < period; i++) {
        const val = candles[validStartIndex + i][sourceKey];
        currentTotalSum += val;
        currentWeightedSum += val * (i + 1);
    }
    candles[validStartIndex + period - 1][targetKey] = currentWeightedSum / weightSum;

    // Швидкий прохід ковзним вікном для решти масиву
    for (let i = validStartIndex + period; i < n; i++) {
        const newPrice = candles[i][sourceKey];
        const oldPrice = candles[i - period][sourceKey];
        
        currentWeightedSum = currentWeightedSum - currentTotalSum + (newPrice * period);
        currentTotalSum = currentTotalSum - oldPrice + newPrice;
        
        candles[i][targetKey] = currentWeightedSum / weightSum;
    }
}

// 2. Функція HMA, яка генерує унікальні ключі для тимчасових даних
function applyHMA(candles, period, sourceKey, targetKey) {
    const nHalf = Math.floor(period / 2);
    const nSqrt = Math.floor(Math.sqrt(period));
    
    // Унікальні ключі, щоб дані HMA25 не затерли дані HMA100
    const keyHalf = `_wmaHalf_${period}`;
    const keyFull = `_wmaFull_${period}`;
    const keyDiff = `_hmaDiff_${period}`;

    // Мутуємо масив, додаючи тимчасові WMA
    applyWMA(candles, nHalf, sourceKey, keyHalf);
    applyWMA(candles, period, sourceKey, keyFull);

    // Рахуємо різницю
    for (let i = 0; i < candles.length; i++) {
        const valHalf = candles[i][keyHalf];
        const valFull = candles[i][keyFull];
        if (valHalf !== undefined && valFull !== undefined) {
            candles[i][keyDiff] = (2 * valHalf) - valFull;
        }
    }

    // Фінальний WMA лягає у targetKey (наприклад, 'hma25')
    applyWMA(candles, nSqrt, keyDiff, targetKey);
}

function applyDeviationCloud(candles, emaLength, basisMode) {
    if (candles.length < emaLength * 2) return;

    const stDevPeriod = emaLength * 2;
    const R1 = 0.92, R2 = 2.0, L4 = 3.8, L5 = 5.5, L6 = 6.0, L8 = 8.0;

    // 1. Рахуємо центральну лінію (EMA або VWAP)
    if (basisMode === 'vwap') {
        // VWAP: Cumulative (Price * Volume) / Cumulative Volume
        // Використовуємо rolling VWAP з вікном = emaLength для подібної "плавності"
        let cumPV = 0, cumVol = 0;
        for (let i = 0; i < candles.length; i++) {
            const c = candles[i];
            const typicalPrice = (c.high + c.low + c.close) / 3;
            const vol = c.volume || 0;
            cumPV += typicalPrice * vol;
            cumVol += vol;

            // Rolling window: віднімаємо старий елемент
            if (i >= emaLength) {
                const old = candles[i - emaLength];
                const oldTP = (old.high + old.low + old.close) / 3;
                cumPV -= oldTP * (old.volume || 0);
                cumVol -= (old.volume || 0);
            }

            c.threeEma = (cumVol > 0) ? (cumPV / cumVol) : c.close;
        }
    } else {
        // EMA (default)
        const k = 2 / (emaLength + 1);
        let currentEma = null;
        for (let i = 0; i < candles.length; i++) {
            const close = candles[i].close;
            if (currentEma === null) currentEma = close;
            else currentEma = (close - currentEma) * k + currentEma;
            candles[i].threeEma = currentEma;
        }
    }

    // 2. Рахуємо StDev (Ковзне вікно) та лінії
    let sum = 0, sumSq = 0;
    
    for (let i = 0; i < candles.length; i++) {
        const c = candles[i];
        sum += c.close;
        sumSq += c.close * c.close;

        if (i >= stDevPeriod) {
            const oldClose = candles[i - stDevPeriod].close;
            sum -= oldClose;
            sumSq -= oldClose * oldClose;
        }

        if (i >= stDevPeriod - 1) {
            const mean = sum / stDevPeriod;
            // Захист від похибок IEEE 754 float за допомогою Math.max(0, ...)
            const variance = Math.max(0, (sumSq / stDevPeriod) - (mean * mean));
            const stdev = Math.sqrt(variance);
            const cloudSize = stdev / 4;

            c.devCloudSize = cloudSize;

            if (cloudSize > 0) {
                const ema = c.threeEma;
                c.devL1 = ema - cloudSize * R1; c.devH1 = ema + cloudSize * R1;
                c.devL2 = ema - cloudSize * R2; c.devH2 = ema + cloudSize * R2;
                c.devL4 = ema - cloudSize * L4; c.devH4 = ema + cloudSize * L4;
                c.devL5 = ema - cloudSize * L5; c.devH5 = ema + cloudSize * L5;
                c.devL6 = ema - cloudSize * L6; c.devH6 = ema + cloudSize * L6;
                c.devL8 = ema - cloudSize * L8; c.devH8 = ema + cloudSize * L8;
            }
        }
    }
}

// 3. Головна функція індикаторів (Параметризована через indicatorConfig)
function calculateIndicators(candles) {
    if (!candles || candles.length === 0) return candles;
    
    const cfg = indicatorConfig;
    const periodSMA = cfg.sma.length;
    const periodBB = cfg.bb.length;
    const stdDevMult = cfg.bb.multiplier;

    // 1. Рахуємо прості індикатори за один прохід
    for (let i = 0; i < candles.length; i++) {
        // SMA
        if (i >= periodSMA - 1) {
            let sum = 0;
            for (let j = 0; j < periodSMA; j++) sum += candles[i - j].close;
            candles[i].sma7 = sum / periodSMA;
        }
        
        // Bollinger Bands
        if (i >= periodBB - 1) {
            let sum = 0;
            for (let j = 0; j < periodBB; j++) sum += candles[i - j].close;
            const mean = sum / periodBB;
            candles[i].bbMiddle = mean;
            
            let sqDiff = 0;
            for (let j = 0; j < periodBB; j++) {
                sqDiff += Math.pow(candles[i - j].close - mean, 2);
            }
            const stdDev = Math.sqrt(sqDiff / periodBB);
            candles[i].bbUpper = mean + stdDev * stdDevMult;
            candles[i].bbLower = mean - stdDev * stdDevMult;
        }
    }

    // 2. Рахуємо всі HMA з параметрами з конфігу
    applyHMA(candles, cfg.hma25.length, 'close', 'hma25');
    applyHMA(candles, cfg.hma35.length, 'close', 'hma35');
    applyHMA(candles, cfg.hma55.length, 'close', 'hma55');
    applyHMA(candles, 80, 'close', 'hma80');
    applyHMA(candles, cfg.hma100.length, 'close', 'hma100');

    // 3. Deviation Cloud з параметрами з конфігу
    applyDeviationCloud(candles, cfg.devCloud.emaLength, cfg.devCloud.basisMode);

    return candles; // Повертаємо той самий мутований масив
}

function mapCandleData(raw) {
    if (!Array.isArray(raw)) return [];
    const result = [];
    for (let i = 0; i < raw.length; i++) {
        const d = raw[i];
        if (!Array.isArray(d) || d.length < 6) continue;
        const time = d[0] / 1000;
        const open = parseFloat(d[1]);
        const high = parseFloat(d[2]);
        const low = parseFloat(d[3]);
        const close = parseFloat(d[4]);
        const volume = parseFloat(d[5]);
        // Валідація: пропускаємо свічки з NaN/Infinity
        if (!isFinite(time) || !isFinite(open) || !isFinite(high) || !isFinite(low) || !isFinite(close) || !isFinite(volume)) continue;
        if (time <= 0 || open <= 0 || high <= 0 || low <= 0 || close <= 0) continue;
        result.push({ time, open, high, low, close, volume });
    }
    return result;
}

// === MARKET LOGIC ===

// Допоміжна: витягти precision з PRICE_FILTER tickSize (для Spot)
function getPrecisionFromTickSize(filters) {
    if (!Array.isArray(filters)) return 8;
    const priceFilter = filters.find(f => f.filterType === 'PRICE_FILTER');
    if (!priceFilter || !priceFilter.tickSize) return 8;
    // tickSize: "0.01000000" → precision = 2
    const tickStr = priceFilter.tickSize;
    const dotIndex = tickStr.indexOf('.');
    if (dotIndex === -1) return 0;
    const decimals = tickStr.slice(dotIndex + 1);
    // Знаходимо позицію останньої значущої цифри (не '0')
    let precision = 0;
    for (let i = 0; i < decimals.length; i++) {
        if (decimals[i] !== '0') precision = i + 1;
    }
    return precision;
}

// Допоміжна: побудувати coin-об'єкт для Futures/Spot з даних exchangeInfo
function buildExchangeInfoCoins(symbols, now, options) {
    const { quoteAsset, filterFn, getSymbol, getDispName, getPrecision, getListingTime } = options;
    
    return symbols
        .filter(s => {
            if (s.status !== 'TRADING') return false;
            if (quoteAsset && s.quoteAsset !== quoteAsset) return false;
            if (filterFn && !filterFn(s)) return false;
            return true;
        })
        .map(s => {
            const listingTime = getListingTime(s);
            const age = now - listingTime;
            let group = 2; // OLD за замовчуванням
            if (listingTime > 0 && age < NEW_COIN_PERIOD_MS) group = 0;
            else if (listingTime > 0 && age < MID_COIN_PERIOD_MS) group = 1;
            return {
                symbol: getSymbol(s),
                dispName: getDispName(s),
                alphaId: null,
                listingDate: listingTime,
                precision: getPrecision(s),
                isNew: group === 0,
                isMid: group === 1,
                group: group
            };
        })
        .sort((a, b) => (a.group !== b.group) ? a.group - b.group : a.dispName.localeCompare(b.dispName));
}

async function fetchAllMarkets() {
    try {
        const now = Date.now();

        // === Паралельне завантаження всіх трьох ринків ===
        const [alphaResult, futuresResult, spotResult] = await Promise.allSettled([
            // ALPHA
            fetchWithTimeout(ENDPOINTS.ALPHA_LIST, {}, 12000).then(r => r.json()),
            // FUTURES
            fetchWithTimeout(ENDPOINTS.FUTURES_INFO, {}, 12000).then(r => r.json()),
            // SPOT
            fetchWithTimeout(ENDPOINTS.SPOT_INFO, {}, 12000).then(r => r.json())
        ]);

        // --- ALPHA ---
        if (alphaResult.status === 'fulfilled') {
            const alphaList = alphaResult.value.data || [];
            marketLists.ALPHA = alphaList
                .filter(c => !c.cexOffDisplay && !c.offline)
                .map(c => {
                    const listingTime = c.listingTime || 0;
                    const age = now - listingTime;
                    let group = 2;
                    if (age < NEW_COIN_PERIOD_MS) group = 0;
                    else if (age < MID_COIN_PERIOD_MS) group = 1;
                    return {
                        symbol: c.symbol, dispName: c.symbol, alphaId: c.alphaId,
                        listingDate: listingTime, precision: (c.tradeDecimal !== undefined) ? c.tradeDecimal : 4,
                        isNew: group === 0, isMid: group === 1, group: group
                    };
                })
                .sort((a, b) => (a.group !== b.group) ? a.group - b.group : a.dispName.localeCompare(b.dispName));
        } else {
            console.error('Failed to load ALPHA market:', alphaResult.reason);
        }

        // --- FUTURES ---
        if (futuresResult.status === 'fulfilled') {
            const futuresData = futuresResult.value;
            marketLists.FUTURES = buildExchangeInfoCoins(futuresData.symbols || [], now, {
                quoteAsset: 'USDT',
                filterFn: (s) => s.contractType === 'PERPETUAL',
                getSymbol: (s) => s.symbol,
                getDispName: (s) => s.baseAsset,
                getPrecision: (s) => s.pricePrecision || 4,
                getListingTime: (s) => s.onboardDate || 0
            });
        } else {
            console.error('Failed to load FUTURES market:', futuresResult.reason);
        }

        // --- SPOT ---
        if (spotResult.status === 'fulfilled') {
            const spotData = spotResult.value;
            marketLists.SPOT = buildExchangeInfoCoins(spotData.symbols || [], now, {
                quoteAsset: 'USDT',
                filterFn: (s) => s.isSpotTradingAllowed === true,
                getSymbol: (s) => s.symbol,
                getDispName: (s) => s.baseAsset,
                getPrecision: (s) => getPrecisionFromTickSize(s.filters),
                // Spot не має onboardDate — ставимо 0 (усі будуть group=2 "OLD")
                getListingTime: (s) => 0
            });
        } else {
            console.error('Failed to load SPOT market:', spotResult.reason);
        }

        // Вважаємо завантаженим, якщо хоча б один ринок завантажився
        const anyLoaded = marketLists.ALPHA.length > 0 || marketLists.FUTURES.length > 0 || marketLists.SPOT.length > 0;
        if (anyLoaded) {
            areMarketsLoaded = true;
            self.postMessage({ type: 'COINS_LISTS', data: marketLists });
        } else {
            // Жоден ринок не завантажився — повторюємо через 10с
            console.error('No markets loaded, retrying in 10s...');
            setTimeout(fetchAllMarkets, 10000);
        }
    } catch (e) {
        console.error('Fetch Markets Error:', e);
        setTimeout(fetchAllMarkets, 10000);
    }
}

async function fetchCandles(chartId, market, symbol, timeframe) {
    if (!areMarketsLoaded) {
        setTimeout(() => fetchCandles(chartId, market, symbol, timeframe), 1000);
        return;
    }

    const coinList = marketLists[market];
    const coin = coinList ? coinList.find(c => c.symbol === symbol) : null;
    
    if (!coin) {
        self.postMessage({ type: 'DATA_ERROR', chartId, symbol, reason: 'Symbol not found' });
        return;
    }

    // Скасування попереднього запиту (з безпечним доступом)
    if (activeSubs[chartId] && activeSubs[chartId].controller) {
        activeSubs[chartId].controller.abort();
    }
    const controller = new AbortController(); // Створюємо новий контролер

    // Оновлюємо стан підписки ОДРАЗУ
    activeSubs[chartId] = { 
        market,
        symbol, 
        timeframe, 
        precision: coin.precision, 
        alphaId: coin.alphaId, 
        buffer: [],
        isLoading: true,
        controller: controller // Зберігаємо контролер
    };

    const url = getKlinesUrl(market, symbol, timeframe, 1000, coin.alphaId);

    try {
        // 🔥 Передаємо signal у fetchWithTimeout
        const res = await fetchWithTimeout(url, { signal: controller.signal }, 10000).then(r => r.json());
        
        // 🔥 FIX: Якщо після fetch символ змінився (користувач клацнув інший), виходимо
        if (activeSubs[chartId].symbol !== symbol) return;

        const raw = (res.data || res); 
        const data = mapCandleData(raw);

        if (data.length === 0) {
            if(activeSubs[chartId].symbol === symbol) activeSubs[chartId].isLoading = false; 
            return;
        }

        const enriched = calculateIndicators(data);
        
        // 🔥 FIX: Фінальна перевірка перед відправкою (чи актуальна ще монета)
        if (activeSubs[chartId].symbol === symbol) {
            activeSubs[chartId].buffer = data; 
            activeSubs[chartId].isLoading = false;
            
            self.postMessage({ 
                type: 'CANDLES_DATA', 
                chartId, 
                data: enriched, 
                precision: coin.precision, 
                symbol,
                market
            });
        }

    } catch (e) {
        if (e.name === 'AbortError') {
            // Тихо ігноруємо, це штатна поведінка
            return;
        } else {
            self.postMessage({ type: 'DATA_ERROR', chartId, symbol, reason: 'Network Error' });
        }
        // Скидаємо прапорець, тільки якщо це поточна монета
        if(activeSubs[chartId] && activeSubs[chartId].symbol === symbol) {
            activeSubs[chartId].isLoading = false;
        }
    }
}

// Допоміжна функція для мікро-затримок (Jitter)
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function runRealtimeLoop() {
    if (isPolling) return;
    isPolling = true;

    const loop = async () => {
        try {
            const targets = [];
            // Беремо тільки активні, не завантажувані (динамічно по всіх графіках)
            Object.keys(activeSubs).forEach(id => {
                const sub = activeSubs[id];
                if (sub && sub.symbol && !sub.isLoading && !sub.historyMode) {
                    targets.push({ id: parseInt(id), ...sub });
                }
            });

            if (targets.length === 0) return;

            const uniqueRequests = new Map();
            targets.forEach(t => {
                const key = `${t.market}_${t.symbol}_${t.timeframe}`; 
                if (!uniqueRequests.has(key)) {
                    uniqueRequests.set(key, { 
                        market: t.market, symbol: t.symbol, timeframe: t.timeframe, alphaId: t.alphaId, chartIds: [t.id] 
                    });
                } else {
                    uniqueRequests.get(key).chartIds.push(t.id);
                }
            });

            const promises = Array.from(uniqueRequests.values()).map(async (req) => {
                try {
                    // НАДІЙНІСТЬ: Додаємо мікро-затримку 0...100 мс перед кожним паралельним запитом
                    await sleep(Math.random() * 100);

                    const url = getKlinesUrl(req.market, req.symbol, req.timeframe, 5, req.alphaId);
                    // Короткий таймаут для ріалтайму
                    const res = await fetchWithTimeout(url, {}, 3000).then(r => r.json());
                    const raw = (res.data || res);
                    const newCandles = mapCandleData(raw);

                    if (newCandles.length > 0) {
                        req.chartIds.forEach(id => {
                            const sub = activeSubs[id];
                            if (!sub || sub.symbol !== req.symbol) return;
                            if (!sub.buffer || sub.buffer.length === 0) return;
                            // Подвійна перевірка: historyMode міг увімкнутись після формування targets
                            if (sub.historyMode) return;

                            let updated = false;
                            newCandles.forEach(nc => {
                                const lastCandle = sub.buffer[sub.buffer.length - 1];
                                if (nc.time > lastCandle.time) {
                                    sub.buffer.push(nc);
                                    updated = true;
                                } else if (nc.time === lastCandle.time) {
                                    // СТАЛО (Додаємо перевірку high та low):
                                    if (
                                        Math.abs(nc.close - lastCandle.close) > Number.EPSILON || 
                                        Math.abs(nc.volume - lastCandle.volume) > Number.EPSILON ||
                                        Math.abs(nc.high - lastCandle.high) > Number.EPSILON ||
                                        Math.abs(nc.low - lastCandle.low) > Number.EPSILON
                                    ) {
                                        sub.buffer[sub.buffer.length - 1] = nc;
                                        updated = true;
                                    }
                                }
                            });

                            if (updated) {
                                if (sub.buffer.length > MAX_BUFFER_SIZE) {
                                    sub.buffer = sub.buffer.slice(sub.buffer.length - MAX_BUFFER_SIZE);
                                    incrementalState[id] = null; // Скидаємо стейт після trim
                                }
                                
                                // === ІНКРЕМЕНТАЛЬНЕ ОНОВЛЕННЯ ===
                                const buf = sub.buffer;
                                const cfg = indicatorConfig;
                                const lastIdx = buf.length - 1;
                                
                                // Інкрементальний SMA для останніх свічок
                                const pSMA = cfg.sma.length;
                                if (lastIdx >= pSMA - 1) {
                                    let smaSum = 0;
                                    for (let si = 0; si < pSMA; si++) smaSum += buf[lastIdx - si].close;
                                    buf[lastIdx].sma7 = smaSum / pSMA;
                                }
                                
                                // Інкрементальний BB
                                const pBB = cfg.bb.length;
                                const bbMult = cfg.bb.multiplier;
                                if (lastIdx >= pBB - 1) {
                                    let bbSum = 0;
                                    for (let si = 0; si < pBB; si++) bbSum += buf[lastIdx - si].close;
                                    const bbMean = bbSum / pBB;
                                    buf[lastIdx].bbMiddle = bbMean;
                                    let bbSqDiff = 0;
                                    for (let si = 0; si < pBB; si++) bbSqDiff += Math.pow(buf[lastIdx - si].close - bbMean, 2);
                                    const bbStd = Math.sqrt(bbSqDiff / pBB);
                                    buf[lastIdx].bbUpper = bbMean + bbStd * bbMult;
                                    buf[lastIdx].bbLower = bbMean - bbStd * bbMult;
                                }
                                
                                // HMA та DevCloud потребують глибшого lookback
                                const startIndex = Math.max(0, buf.length - CALC_LOOKBACK);
                                const sliceData = buf.slice(startIndex);
                                
                                // HMA (потребує WMA chains)
                                applyHMA(sliceData, cfg.hma25.length, 'close', 'hma25');
                                applyHMA(sliceData, cfg.hma35.length, 'close', 'hma35');
                                applyHMA(sliceData, cfg.hma55.length, 'close', 'hma55');
                                applyHMA(sliceData, 80, 'close', 'hma80');
                                applyHMA(sliceData, cfg.hma100.length, 'close', 'hma100');
                                
                                // DevCloud (потребує EMA/VWAP chain + StDev window)
                                applyDeviationCloud(sliceData, cfg.devCloud.emaLength, cfg.devCloud.basisMode);
                                
                                const updates = [];
                                const calculatedMap = new Map();
                                sliceData.forEach(c => calculatedMap.set(c.time, c));
                                
                                newCandles.forEach(nc => {
                                    const enriched = calculatedMap.get(nc.time);
                                    if (enriched) updates.push(enriched);
                                });

                                if (updates.length > 0) {
                                    self.postMessage({
                                        type: 'REALTIME_UPDATE',
                                        chartId: id,
                                        data: updates,
                                        symbol: req.symbol,
                                        market: req.market
                                    });
                                }
                            }
                        });
                    }
                } catch (e) { 
                    if (e.message && e.message.includes('429')) {
                        await sleep(5000);
                    }
                }
            });
            await Promise.all(promises);
        } catch (fatalError) {
            console.error('Fatal RT Loop Error:', fatalError);
        } finally {
            // НАДІЙНІСТЬ: Базові 2900 мс + випадкові 0...300 мс
            const nextLoopDelay = 2900 + (Math.random() * 300);
            setTimeout(loop, nextLoopDelay);
        }
    };
    loop();
}

// === HISTORY WINDOW LOADING ===
async function fetchHistoryWindow(chartId, market, symbol, timeframe, startTimeMs, endTimeMs) {
    const coinList = marketLists[market];
    const coin = coinList ? coinList.find(c => c.symbol === symbol) : null;
    
    if (!coin) {
        self.postMessage({ type: 'DATA_ERROR', chartId, symbol, reason: 'Symbol not found for history' });
        return;
    }

    // Перевірка лістингу: якщо startTime < listingDate — обрізаємо
    const listingMs = coin.listingDate || 0;
    let effectiveStart = startTimeMs;
    if (listingMs > 0 && effectiveStart < listingMs) {
        effectiveStart = listingMs;
    }

    // Якщо після обрізки startTime >= endTime — нічого не вантажимо
    if (effectiveStart >= endTimeMs) {
        self.postMessage({ type: 'HISTORY_WINDOW_DATA', chartId, data: [], symbol, market, precision: coin.precision });
        return;
    }

    const sub = activeSubs[chartId];
    if (!sub || sub.symbol !== symbol) return;

    const url = getKlinesUrl(market, symbol, timeframe, 1000, coin.alphaId, effectiveStart, endTimeMs);

    try {
        const res = await fetchWithTimeout(url, {}, 12000).then(r => r.json());
        
        // Перевірка актуальності (чи не змінив користувач монету поки чекали)
        if (!activeSubs[chartId] || activeSubs[chartId].symbol !== symbol) return;

        const raw = (res.data || res);
        const data = mapCandleData(raw);

        if (data.length === 0) {
            self.postMessage({ type: 'HISTORY_WINDOW_DATA', chartId, data: [], symbol, market, precision: coin.precision });
            return;
        }

        const enriched = calculateIndicators(data);

        // Оновлюємо буфер воркера новими даними (заміна, не append)
        activeSubs[chartId].buffer = data;
        incrementalState[chartId] = null;

        self.postMessage({
            type: 'HISTORY_WINDOW_DATA',
            chartId,
            data: enriched,
            precision: coin.precision,
            symbol,
            market
        });

    } catch (e) {
        if (e.name !== 'AbortError') {
            self.postMessage({ type: 'DATA_ERROR', chartId, symbol, reason: 'History load error' });
        }
    }
}

self.onmessage = async (e) => {
    const { type, payload } = e.data;
    if (type === 'INIT') {
        const count = (payload && payload.chartCount) ? payload.chartCount : 2;
        initChartSlots(count);
        if (payload && payload.indicatorConfig) {
            Object.assign(indicatorConfig, payload.indicatorConfig);
        }
        await fetchAllMarkets();
        runRealtimeLoop(); 
    }
    if (type === 'LOAD_CHART') {
        const { chartId, market, symbol, timeframe } = payload;
        incrementalState[chartId] = null;
        // Скидаємо historyMode при завантаженні нової монети/таймфрейму
        if (activeSubs[chartId]) activeSubs[chartId].historyMode = false;
        await fetchCandles(chartId, market, symbol, timeframe);
    }
    if (type === 'SET_HISTORY_MODE') {
        const { chartId, enabled } = payload;
        if (activeSubs[chartId]) {
            activeSubs[chartId].historyMode = enabled;
        }
    }
    if (type === 'LOAD_HISTORY_WINDOW') {
        const { chartId, market, symbol, timeframe, startTime, endTime } = payload;
        await fetchHistoryWindow(chartId, market, symbol, timeframe, startTime, endTime);
    }
    if (type === 'UPDATE_INDICATOR_CONFIG') {
        if (payload) {
            Object.assign(indicatorConfig, payload);
        }
        Object.keys(activeSubs).forEach(idStr => {
            const id = parseInt(idStr);
            const sub = activeSubs[id];
            if (!sub || !sub.buffer || sub.buffer.length === 0) return;
            incrementalState[id] = null;
            const enriched = calculateIndicators(sub.buffer);
            self.postMessage({
                type: 'CANDLES_DATA',
                chartId: id,
                data: enriched,
                precision: sub.precision,
                symbol: sub.symbol,
                market: sub.market
            });
        });
    }
};