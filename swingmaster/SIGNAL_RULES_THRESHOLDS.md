# Swingmaster Signal Rules and Thresholds (Code-Based)

Tämä dokumentti kuvaa **täsmälleen koodin perusteella** signaalien syntylogiikan, käytetyt muuttujat ja raja-arvot.

Lähteet:
- `swingmaster/app_api/providers/osakedata_signal_provider_v2.py`
- `swingmaster/app_api/providers/signals_v2/trend_started.py`
- `swingmaster/app_api/providers/signals_v2/trend_matured.py`
- `swingmaster/app_api/providers/signals_v2/stabilization_confirmed.py`
- `swingmaster/app_api/providers/signals_v2/entry_setup_valid.py`
- `swingmaster/app_api/providers/signals_v2/invalidated.py`
- `swingmaster/app_api/providers/signals_v2/dow_structure.py`

## 1. Providerin yleiset asetukset (v2)

Provider-luokka: `OsakeDataSignalProviderV2`.

Parametrit (default):
- `sma_window=20`
- `momentum_lookback=1`
- `matured_below_sma_days=5`
- `atr_window=14`
- `stabilization_days=5`
- `atr_pct_threshold=0.03`
- `range_pct_threshold=0.05`
- `entry_sma_window=5`
- `invalidation_lookback=10`
- `require_row_on_date=False`
- `dow_window=3`
- `dow_use_high_low=True`
- `dow_sensitive_down_reset=False`
- `SAFETY_MARGIN_ROWS=2`

### 1.1 Vaadittu historian pituus
Provider laskee minimirivimäärän näin:

`required_rows = max(
  sma_window + momentum_lookback,
  sma_window + 5,
  atr_window + 1,
  max(stabilization_days + 1, entry_sma_window),
  invalidation_lookback + 1,
  (2 * dow_window) + 1,
  SMA_LEN + REGIME_WINDOW - 1,
  SMA_LEN + SLOPE_LOOKBACK,
  BREAK_LOW_WINDOW + 1
) + SAFETY_MARGIN_ROWS`

Jos `len(ohlc) < required_rows` -> `DATA_INSUFFICIENT`.

## 2. TREND_STARTED

Tiedosto: `trend_started.py`.

Vakiot:
- `SMA_LEN = 20`
- `SLOPE_LOOKBACK = 5`
- `REGIME_WINDOW = 30`
- `ABOVE_RATIO_MIN = 0.70`
- `BREAK_LOW_WINDOW = 10`
- `DEBOUNCE_DAYS = 5`

### 2.1 Ehdot
Signaali syntyy, jos kaikki toteutuvat:
1. Riittävä data (`min_required`):
   - `max(SMA_LEN + REGIME_WINDOW - 1, SMA_LEN + SLOPE_LOOKBACK, SMA_LEN + DEBOUNCE_DAYS + 1, BREAK_LOW_WINDOW + 1)`
2. `sma20` saatavilla.
3. Regime ok:
   - `above_ratio = (# päivät i in [0..29] joissa closes[i] > sma20[i]) / 30`
   - `above_ratio >= 0.70`
   - `sma20_slope = sma20[0] - sma20[5] > 0`
4. Cross ehto:
   - `yesterday_close >= yesterday_sma`
   - `today_close < today_sma`
5. Debounce:
   - yhdelläkään `i = 1..(1+DEBOUNCE_DAYS)` ei saa olla `closes[i] < sma20[i]`.
6. Breakdown:
   - `today_close < min(closes[1 : 1 + BREAK_LOW_WINDOW])`.

## 3. TREND_MATURED

Tiedosto: `trend_matured.py`.

Vakiot:
- `SMA_LEN = 20`
- `STRUCT_WINDOW = 15`
- `NEW_LOW_LOOKBACK = 10`
- `DRAW_REF_LOOKBACK_A = 20`
- `DRAW_REF_LOOKBACK_B = 5`
- `DRAW_MIN_DD = 0.10`
- `MIN_AGE_DAYS = 10`
- `MOMENTUM_WINDOW = 20`
- `MOMENTUM_NEWLOW_COUNT = 3`
- `MOMENTUM_DROP_MAX = 0.02`

### 3.1 Ehdot
Signaali syntyy, jos `structure_ok AND time_ok AND momentum_ok`:

1. `structure_ok`:
   - `structure_new_lows`: vähintään 2 uutta low:ta `STRUCT_WINDOW`-ikkunassa, missä uusi low määritellään ehdolla `closes[idx] < min(prior NEW_LOW_LOOKBACK closes)`.
   - TAI `structure_drawdown`:
     - `ref_high = max(closes[5:21])`
     - `drawdown = (ref_high - closes[0]) / ref_high`
     - `drawdown >= 0.10`
2. `time_ok`:
   - viime `MIN_AGE_DAYS=10` päivän aikana close SMA20:n alapuolella vähintään 70% päivistä.
3. `momentum_ok`:
   - viime `MOMENTUM_WINDOW`-ikkunasta löytyy vähintään 3 new low -indeksiä.
   - valitaan kronologisesti viimeiset kolme low:ta `l1,l2,l3`.
   - `step1_pct = abs(l2-l1)/l1 <= 0.02`
   - `step2_pct = abs(l3-l2)/l2 <= 0.02`

## 4. STABILIZATION_CONFIRMED

Tiedosto: `stabilization_confirmed.py`.

Vakiot:
- `BASELINE_WINDOW = 20`
- `STAB_WINDOW = 7`
- `NO_NEW_LOW_WINDOW = 7`
- `SIGNIFICANT_LOW_EPS = 0.003`
- `RANGE_SHRINK_RATIO = 0.75`
- `WIDE_DAY_MULT = 1.5`
- `WIDE_DAY_RATIO_MAX = 0.20`
- `CLOSE_UPPER_FRAC_MIN = 0.55`
- `CLOSE_UPPER_DAYS_MIN = 3`
- `SWEEP_MAX_COUNT = 1`

### 4.1 Ehdot
Signaali syntyy, jos kaikki toteutuvat:
1. `range_shrink_ok`:
   - `recent_range = (high-low)/close` viimeiset 7 päivää
   - `baseline_range` seuraavat 20 päivää
   - `median(recent_range) <= median(baseline_range) * 0.75`
2. `wide_days_ok`:
   - lasketaan recent-ikkunan päivät, joilla range >= baseline_median * 1.5
   - `wide_days / 7 <= 0.20`
3. `no_new_low_ok`:
   - `significant_new_low_count == 0`, missä significant low on
     `low_d < ref_low * (1 - 0.003)`
   - ja `sweep_count <= 1`, missä sweep on
     `ref_low*(1-0.003) <= low_d < ref_low`
4. `upper_closes_ok`:
   - close-position `(close-low)/(high-low) >= 0.55`
   - toteutuu vähintään 3 päivänä 7:stä.

## 5. ENTRY_SETUP_VALID

Tiedosto: `entry_setup_valid.py`.

Vakiot:
- `SMA_LEN = 20`
- `BASE_WINDOW = 10`
- `BASE_MAX_WIDTH_PCT = 0.06`
- `LOW_DRIFT_EPS = 0.003`
- `CLOSE_POS_MIN = 0.55`
- `ATR_LEN = 14`
- `RISK_ATR_MAX = 2.5`
- `RISK_PCT_MAX = 0.06`
- `SUPPORT_LOOKBACK = 3`
- `SUPPORT_BREAK_EPS = 0.003`

### 5.1 Ehdot
Signaali syntyy, jos setup + risk + support täyttyvät.

Setup hyväksytään, jos jompikumpi:
1. `base_range`:
   - `width_pct = (max(high[0:10]) - min(low[0:10])) / close[0] <= 0.06`
   - low drift -ehto ei riko (`min_second` ei saa laskea liikaa `min_first`-tasosta):
     - `min_second >= min_first * (1 - 0.003)`
   - invalidation = `window_low`
2. `reclaim_ma20`:
   - `closes[1] <= sma20[1]` ja `closes[0] > sma20[0]`
   - close position tänään >= `0.55`
   - invalidation = `min(lows[:6])`

Lisäksi:
- `entry_price = closes[0] > invalidation_level`
- Riski:
  - jos ATR14 saatavilla: `(entry - invalidation)/atr14 <= 2.5`
  - muuten: `(entry - invalidation)/entry <= 0.06`
- Support:
  - viime 3 päivän close ei saa rikkoa invalidation-tasoa alle
    `invalidation*(1-0.003)`.

## 6. INVALIDATED

Tiedosto: `invalidated.py`.

Ehto:
- jos `len(lows) >= invalidation_lookback + 1`
- ja `lows[0] < min(lows[1 : invalidation_lookback+1])`
-> `INVALIDATED = True`.

Providerin lisälogiikka:
- kun `INVALIDATED=True`, poistetaan saman päivän signaaleista:
  - `STABILIZATION_CONFIRMED`
  - `ENTRY_SETUP_VALID`

## 7. DOW-rakenne signaalit

Tiedosto: `dow_structure.py` / `compute_dow_signal_facts`.

### 7.1 Perusasetukset
- `EPS_PCT = 0.0001`
- `window` tulee provider-parametrista (`dow_window`, default 3).

### 7.2 Faktojen muodostus
Koodin palauttamat signal-faktat:

1. Trendi (aina yksi):
- `DOW_TREND_UP` tai `DOW_TREND_DOWN` tai `DOW_TREND_NEUTRAL`

2. Viime low/high labelit:
- low: `DOW_LAST_LOW_LL` / `DOW_LAST_LOW_HL` / `DOW_LAST_LOW_L`
- high: `DOW_LAST_HIGH_HH` / `DOW_LAST_HIGH_LH` / `DOW_LAST_HIGH_H`

3. Uudet murtumat:
- `DOW_NEW_LL` kun viime low-label = `LL` ja uusi low rikkoo aiemman low:n yli `EPS_PCT` marginaalin
- `DOW_NEW_HH` vastaavasti high-puolella

4. Trendimuutokset `as_of_date`-päivänä:
- `DOW_TREND_CHANGE_UP_TO_NEUTRAL`
- `DOW_TREND_CHANGE_DOWN_TO_NEUTRAL`
- `DOW_TREND_CHANGE_NEUTRAL_TO_UP`
- `DOW_TREND_CHANGE_NEUTRAL_TO_DOWN`

5. Reset/BoS:
- `DOW_RESET` jos reset-marker osuu `as_of_date`-päivään
- lisäksi:
  - `DOW_BOS_BREAK_DOWN` jos reset tapahtui UP-trendistä
  - `DOW_BOS_BREAK_UP` jos reset tapahtui DOWN-trendistä

## 8. NO_SIGNAL

Provider lisää `NO_SIGNAL`, kun:
- yhtään primäärisignaalia ei syntynyt,
- eikä `INVALIDATED` ole aktiivinen.

## 9. Signaalien keskinäinen prioriteetti provider-tasolla

Providerin sisäinen prioriteettikäyttäytyminen:
- `INVALIDATED` voi kumota saman päivän `STABILIZATION_CONFIRMED` ja `ENTRY_SETUP_VALID`.
- `TREND_STARTED` voi syntyä joko base-logiikasta tai DOW-force-ehdosta.
- DOW-faktat lisätään signal-settiin aina laskennan jälkeen.
- Jos ei primäärisignaaleja, fallback `NO_SIGNAL`.

---

Huomio: tämä dokumentti seuraa nykyistä koodia. Jos vakioita tai ehtoja muutetaan signal-moduuleissa, dokumentti pitää päivittää vastaavasti.
