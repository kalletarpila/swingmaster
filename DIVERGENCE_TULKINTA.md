# Divergenssit - tulkintaopas tietokannan käyttäjälle

## Kenelle tämä dokumentti on

Tämä dokumentti on tarkoitettu henkilölle tai lukukerrokselle, joka tulkitsee `divergence_data`-taulua analyysin, raportoinnin tai muun päätöksenteon taustaksi.

Painotus on tulkinnassa, ei ohjelmistotekniikassa. Mukana on kuitenkin riittävästi täsmällisiä sääntöjä, jotta myös koneellinen lukukerros voi käyttää taulua turvallisesti.

## Mitä divergenssi tässä järjestelmässä tarkoittaa

Divergenssi kuvaa tilannetta, jossa hinnan liike ja RSI:n liike eivät vahvista toisiaan.

Järjestelmässä käytetään neljää päätyyppiä:

- `bullish_strength`: tavallinen bullish divergenssi
- `bearish_strength`: tavallinen bearish divergenssi
- `hidden_bullish_strength`: piilobullish divergenssi
- `hidden_bearish_strength`: piilobearish divergenssi

Yleinen tulkinta:

- Bullish divergenssi viittaa mahdolliseen heikkouden vähenemiseen laskun jälkeen.
- Bearish divergenssi viittaa mahdolliseen nousun heikkenemiseen.
- Hidden bullish viittaa useammin nousevan rakenteen jatkumiseen kuin varsinaiseen pohjakäänteeseen.
- Hidden bearish viittaa useammin laskevan rakenteen jatkumiseen kuin varsinaiseen huippukäänteeseen.

## Laskennan perusidea selkokielellä

### 1) Jokaiselle hinnan päivälle tallennetaan oma rivi

Toisin kuin Dow-rakenne, `divergence_data` ei ole event-pohjainen taulu.

Tauluun tallennetaan rivi jokaiselle käsitellylle tickerin päivälle, ei vain niille päiville joilla signaali on havaittu.

Siksi:

- rivin olemassaolo ei tarkoita, että kyseisellä päivällä olisi divergenssi
- divergenssi on olemassa vasta, jos strength-kenttä on suurempi kuin 0 tai vastaava event-lippu on 1

### 2) RSI lasketaan sulkuhinnoista

RSI lasketaan Wilderin menetelmällä käyttäen oletusperiodia 14.

Siksi divergenssitulkinta tässä järjestelmässä perustuu:

- hintasarjaan
- sulkuhinnoista johdettuun RSI:hin

### 3) Perusdivergenssit käyttävät vain mennyttä dataa

Perus-V1-divergenssi (`bullish_strength`, `bearish_strength`, `hidden_bullish_strength`, `hidden_bearish_strength`) lasketaan vertaamalla tarkastelupäivää vain aiempiin päiviin.

Keskeiset oletusarvot:

- `MIN_HISTORY_DAYS = 30`
- `LOOKBACK_DAYS = 90`
- `CANDIDATE_WINDOW = 10`

Tulkinta:

- ennen kuin historiallista dataa on riittävästi, strength-arvot pysyvät nollassa
- tarkastelupäivän signaali voi käyttää vain aiempia havaintoja, ei tulevia päiviä

### 4) Kandidaattipohjat ja -huiput ovat trailing-tyyppisiä

Perus-V1-logiikassa tarkastellaan, onko aiempi päivä ollut viimeisten 10 päivän sisällä uusi paikallinen pohja tai huippu.

Bullish-puolella vertaillaan paikallisia pohjia:

- hinta tekee alemman pohjan
- RSI tekee korkeamman pohjan

Bearish-puolella vertaillaan paikallisia huippuja:

- hinta tekee korkeamman huipun
- RSI tekee matalamman huipun

Hidden-variantit kääntävät hinnan suunnan jatkotrendin tulkintaa varten:

- hidden bullish: hinta tekee korkeamman pohjan, RSI matalamman pohjan
- hidden bearish: hinta tekee matalamman huipun, RSI korkeamman huipun

### 5) Strength on jatkuva arvo välillä 0..1

Strength ei ole vain kyllä/ei-signaali, vaan voimakkuusmitta.

Arvo muodostuu kolmesta osasta:

- hinnan muutoskomponentti
- RSI-muutoskomponentti
- yliostettu / ylimyyty -komponentti

Painot perus-V1-laskennassa:

- 45 % hinnan muutos
- 45 % RSI:n muutos
- 10 % yliostettu / ylimyyty -tilanne

Käytännön tulkinta:

- lähellä 0 oleva arvo on heikko tai olematon divergenssi
- suurempi arvo kertoo vahvemmasta erosta hinnan ja RSI:n välillä
- maksimi on rajattu välille 0..1

## R2- ja R3-event-liput

Taulussa on myös erilliset lippukentät versiolle, jossa käytetään hinta- ja RSI-pivoteja:

- radius 2: `*_r2`
- radius 3: `*_r3`

Näissä kentissä logiikka on eri kuin perus-V1-strengthissä.

Keskeinen ero:

- event-lippu kirjataan vahvistuspäivälle
- varsinainen vertailun toinen hintapivot voi olla aiemmalla päivällä, joka näkyy kentässä `pivot2_date_r2` tai `pivot2_date_r3`

Siksi koneellisen analyysin pitää erottaa toisistaan:

- eventin kirjauspäivä = taulun `date`
- toisen pivotin päivä = `pivot2_date_r2` tai `pivot2_date_r3`

## Analyysipäivän sääntö — no-lookahead

Tämä on kriittinen sääntö kaikelle tulkinnalle.

Kun divergenssejä tulkitaan analyysipäivälle, saa käyttää vain rivejä, joilla:

```sql
date <= analysis_as_of_date
```

Tämä sääntö on turvallinen, koska:

- perus-V1-strengthit käyttävät vain mennyttä dataa
- R2/R3-event-liput kirjautuvat nimenomaan vahvistuspäivälle taulun `date`-kenttään

Kriittinen lisähuomio R2/R3-kentille:

- `pivot2_date_r2` tai `pivot2_date_r3` ei yksin riitä analyysipäivän rajaksi
- käyttökelpoinen päivä on aina taulun `date`, ei pivotin päivämäärä

Esimerkki:

- `pivot2_date_r3 = 2026-04-10`
- rivin `date = 2026-04-13`

Jos analyysipäivä on `2026-04-11`, tätä R3-signaalia ei saa käyttää, vaikka pivotin päivä on jo menneisyydessä. Vasta `date = 2026-04-13` tekee signaalista käytettävän.

## Miten viimeisin käyttökelpoinen tila haetaan SQL:llä

### Viimeisin divergence-rivi analyysipäivälle

```sql
SELECT *
FROM divergence_data
WHERE ticker = ?
  AND date <= ?
ORDER BY date DESC
LIMIT 1;
```

Tämä palauttaa viimeisimmän tunnetun rivin analyysipäivään asti.

### Viimeisin päivä, jolla oli mikä tahansa divergenssisignaali

```sql
SELECT *
FROM divergence_data
WHERE ticker = ?
  AND date <= ?
  AND (
    COALESCE(bullish_strength, 0) > 0
    OR COALESCE(bearish_strength, 0) > 0
    OR COALESCE(hidden_bullish_strength, 0) > 0
    OR COALESCE(hidden_bearish_strength, 0) > 0
    OR COALESCE(is_bullish_divergence_r2, 0) = 1
    OR COALESCE(is_bearish_divergence_r2, 0) = 1
    OR COALESCE(is_hidden_bullish_divergence_r2, 0) = 1
    OR COALESCE(is_hidden_bearish_divergence_r2, 0) = 1
    OR COALESCE(is_bullish_divergence_r3, 0) = 1
    OR COALESCE(is_bearish_divergence_r3, 0) = 1
    OR COALESCE(is_hidden_bullish_divergence_r3, 0) = 1
    OR COALESCE(is_hidden_bearish_divergence_r3, 0) = 1
  )
ORDER BY date DESC
LIMIT 1;
```

Huomio vanhemmista bool-lipuista:

- `is_bullish_divergence`
- `is_bearish_divergence`
- `is_hidden_bullish_divergence`
- `is_hidden_bearish_divergence`

Nykyisessä V1-toteutuksessa nämä ovat käytännössä vanhempia yhteensopivuuskenttiä, jotka peilaavat R2-lippuja. Niitä ei siksi käytetä erikseen V1-lukukerroksen signaalihakuihin, ellei myöhemmin päätetä toisin.

### Viimeisin bullish-signaali analyysipäivään asti

```sql
SELECT *
FROM divergence_data
WHERE ticker = ?
  AND date <= ?
  AND (
    COALESCE(bullish_strength, 0) > 0
    OR COALESCE(hidden_bullish_strength, 0) > 0
    OR COALESCE(is_bullish_divergence_r2, 0) = 1
    OR COALESCE(is_hidden_bullish_divergence_r2, 0) = 1
    OR COALESCE(is_bullish_divergence_r3, 0) = 1
    OR COALESCE(is_hidden_bullish_divergence_r3, 0) = 1
  )
ORDER BY date DESC
LIMIT 1;
```

## Coverage-tulkinta divergence-taululle

Divergensseille ei ole erillistä status-taulua kuten Dow-rakenteelle. Siksi kattavuus pitää päätellä vertaamalla `divergence_data`-taulua hintadataan.

Ensisijainen sääntö on tämä:

- coverage vertaa aina `divergence_data`-taulun viimeisintä `date`-arvoa `latest_valid_close_date_on_or_before_as_of_date`-arvoon
- vertailukohtana ei käytetä raakaa `MAX(pvm)`-arvoa ilman `close IS NOT NULL` -ehtoa

Määritelmät:

- `latest_valid_close_date_on_or_before_as_of_date = MAX(pvm)`, jossa `close IS NOT NULL` ja `pvm <= analysis_as_of_date`
- `latest_divergence_date_on_or_before_as_of_date = MAX(date)`, jossa `date <= analysis_as_of_date`

Suositeltu käytännön tulkinta:

| Tila | Merkitys | Suositus |
|---|---|---|
| OK | divergence_data:n viimeisin date kattaa tickerin viimeisimmän hinnan päivän | Voi käyttää analyysissa |
| STALE | divergence_data:n viimeisin päivä on vanhempi kuin tickerin viimeisin hinnan päivä | Aja uudelleenlaskenta ennen käyttöä |
| MISSING | tickerille ei ole yhtään divergence-riviä | Älä käytä ennen laskentaa |
| PARTIAL | rivejä on, mutta osa viimeisistä hinnan päivistä puuttuu | Aja inkrementaalinen täydennys |
| ERROR | laskenta epäonnistyi sovellustasolla tai data jäi tyhjäksi poikkeustilanteessa | Tarkista ajoloki tai aja uudelleen |

### Kattavuuden tarkistus SQL:llä

Jos `analysis.db` ja `osakedata.db` ovat eri SQLite-kannoissa, osakedata pitää liittää `ATTACH`-komennolla samaan yhteyteen tai vertailu pitää tehdä kahdella erillisellä kyselyllä.

Esimerkki yhdessä yhteydessä:

```sql
SELECT
    latest_price.ticker,
    latest_price.latest_price_date,
    latest_div.latest_divergence_date,
    CASE
        WHEN latest_div.latest_divergence_date IS NULL THEN 'MISSING'
        WHEN latest_div.latest_divergence_date >= latest_price.latest_price_date THEN 'OK'
        ELSE 'STALE'
    END AS coverage_status
FROM (
  SELECT osake AS ticker, MAX(pvm) AS latest_price_date
    FROM osakedata
  WHERE close IS NOT NULL
    AND pvm <= ?
    GROUP BY osake
) latest_price
LEFT JOIN (
    SELECT ticker, MAX(date) AS latest_divergence_date
    FROM divergence_data
  WHERE date <= ?
    GROUP BY ticker
) latest_div
  ON UPPER(latest_div.ticker) = UPPER(latest_price.ticker);
```

Tämä on suositeltu V1-käytäntö myös historialliselle raportille: coverage tarkistetaan aina analyysipäivään asti, ei koko kannan uusimpaan päivään.

## Taulu: divergence_data

Alla on taulun skeema tulkintamuodossa.

| Kenttä | Tyyppi | Selitys |
|---|---|---|
| ticker | TEXT | Ticker |
| date | TEXT | Päivä, jolle divergence-rivi on kirjattu |
| bullish_strength | REAL | Tavallisen bullish divergenssin voimakkuus välillä 0..1 |
| bearish_strength | REAL | Tavallisen bearish divergenssin voimakkuus välillä 0..1 |
| hidden_bullish_strength | REAL | Hidden bullish divergenssin voimakkuus välillä 0..1 |
| hidden_bearish_strength | REAL | Hidden bearish divergenssin voimakkuus välillä 0..1 |
| rsi | REAL | Päivän RSI-arvo |
| is_bullish_divergence | INTEGER | Vanhempi bool-lippu bullish-divergenssille |
| is_bearish_divergence | INTEGER | Vanhempi bool-lippu bearish-divergenssille |
| is_hidden_bullish_divergence | INTEGER | Vanhempi bool-lippu hidden bullish -divergenssille |
| is_hidden_bearish_divergence | INTEGER | Vanhempi bool-lippu hidden bearish -divergenssille |
| is_bullish_divergence_r2 | INTEGER | Pivot-radius 2 -vahvistettu bullish-eventti |
| is_bearish_divergence_r2 | INTEGER | Pivot-radius 2 -vahvistettu bearish-eventti |
| is_hidden_bullish_divergence_r2 | INTEGER | Pivot-radius 2 -vahvistettu hidden bullish -eventti |
| is_hidden_bearish_divergence_r2 | INTEGER | Pivot-radius 2 -vahvistettu hidden bearish -eventti |
| is_bullish_divergence_r3 | INTEGER | Pivot-radius 3 -vahvistettu bullish-eventti |
| is_bearish_divergence_r3 | INTEGER | Pivot-radius 3 -vahvistettu bearish-eventti |
| is_hidden_bullish_divergence_r3 | INTEGER | Pivot-radius 3 -vahvistettu hidden bullish -eventti |
| is_hidden_bearish_divergence_r3 | INTEGER | Pivot-radius 3 -vahvistettu hidden bearish -eventti |
| pivot_gap | INTEGER | Vanhemman mallin pivot-väli, jos täytetty |
| pivot_drop_pct | REAL | Vanhemman mallin hintamuutos pivotien välillä |
| pivot_gap_r2 | INTEGER | R2-mallissa pivotien välinen etäisyys kaupankäyntipäivinä |
| pivot_drop_pct_r2 | REAL | R2-mallissa hinnan prosentuaalinen muutos pivotien välillä |
| hidden_pivot_gap_r2 | INTEGER | Hidden R2 -mallin pivotien välinen etäisyys |
| hidden_pivot_drop_pct_r2 | REAL | Hidden R2 -mallin hintamuutos pivotien välillä |
| pivot2_date_r2 | TEXT | R2-mallin toisen hintapivotin päivä |
| pivot_gap_r3 | INTEGER | R3-mallissa pivotien välinen etäisyys kaupankäyntipäivinä |
| pivot_drop_pct_r3 | REAL | R3-mallissa hinnan prosentuaalinen muutos pivotien välillä |
| hidden_pivot_gap_r3 | INTEGER | Hidden R3 -mallin pivotien välinen etäisyys |
| hidden_pivot_drop_pct_r3 | REAL | Hidden R3 -mallin hintamuutos pivotien välillä |
| pivot2_date_r3 | TEXT | R3-mallin toisen hintapivotin päivä |

Pääavain:

- `ticker`
- `date`

Tärkeä käytännön seuraus:

- kullakin tickerillä voi olla enintään yksi divergence-rivi per päivä
- sama päivä voi silti sisältää useita samanaikaisia signaaleja eri kentissä

## Kenttien tulkinta käytännössä

### Strength-kentät

Strength-kenttä kertoo jatkuvan vahvuuden eikä vain binääristä tapahtumaa.

Käytännön tulkinta:

- `0.0` = ei signaalia
- pieni positiivinen arvo = heikko signaali
- keskisuuri arvo = analyysin arvoinen signaali
- suuri arvo lähellä 1.0 = poikkeuksellisen vahva epäjatkuvuus hinnan ja RSI:n välillä

Strengthin tarkka raja käytännön päätöksissä on sovelluskohtainen. Kannan näkökulmasta turvallinen sääntö on käyttää aina ehtoa `> 0` signaalin olemassaoloon ja käsitellä voimakkuutta erillisenä priorisointitekijänä.

### R2- ja R3-liput

R2- ja R3-liput ovat binäärisiä vahvistettuja tapahtumia.

Tulkinta:

- `= 1` tarkoittaa, että kyseisen radius-mallin ehto täyttyi vahvistuspäivänä
- `= 0` tarkoittaa, ettei kyseisellä päivällä ole tämän mallin vahvistettua eventtiä

### pivot_gap ja pivot_drop_pct

Nämä kentät auttavat arvioimaan signaalin rakennetta:

- `pivot_gap_*` kertoo, kuinka monta kaupankäyntipäivää pivotien välissä on
- `pivot_drop_pct_*` kertoo hintamuutoksen suuruuden pivotien välillä

Käytännön tulkinta:

- hyvin lyhyt gap voi tarkoittaa herkemmin kohinaa
- suurempi mutta silti sallitun rajan sisällä oleva gap voi viitata rakenteellisesti merkittävämpään havaintoon

## Miten taulua tulkitaan yhdessä muun analyysin kanssa

### 1) Onko tickerillä bullish-divergenssi analyysipäivänä

Käytä joko:

- perusstrengthiä: `bullish_strength > 0` tai `hidden_bullish_strength > 0`
- event-lippuja: `is_bullish_divergence_r2 = 1`, `is_bullish_divergence_r3 = 1`, `is_hidden_bullish_divergence_r2 = 1`, `is_hidden_bullish_divergence_r3 = 1`

### 2) Mikä on viimeisin käyttökelpoinen divergence-tila

Hae viimeisin rivi ennen analyysipäivää ja lue siitä:

- strength-kentät
- rsi
- mahdolliset R2/R3-event-liput

### 3) Onko signaali tuore

Divergenssille ei ole erillistä `confirmed_as_of_date`-kenttää. Tuoreus arvioidaan käytännössä kentän `date` perusteella.

Suositeltu käytännön sääntö ihmislukijalle:

- mitä lähempänä analyysipäivää viimeisin signaali on, sitä ajankohtaisempi se on
- vanha signaali ilman uusia vahvistuksia on enemmän historiallinen konteksti kuin aktiivinen triggeri

Koneelliselle lukukerrokselle tämä kannattaa pitää neutraalina:

- lukukerros voi palauttaa signaalin iän kalenteripäivinä tai kaupankäyntipäivinä
- V1 ei vielä päätä, mikä signaalin ikä tekee siitä aktiivisen tai vanhentuneen

Mahdollisia myöhempiä kenttiä tai johdettuja mittareita ovat esimerkiksi:

- `signal_age_calendar_days`
- `signal_age_trading_days`
- `recent_window_days`

## Fundamenttianalyysin tulkintamatriisi

Divergenssi ei yksinään tuota ostosuositusta. Se antaa teknistä lisäkontekstia sille, tukeeko hintakäyttäytyminen fundamenttikuvaa vai ei.

| Tilanne | Tulkinta |
|---|---|
| `bullish_strength > 0` analyysipäivällä tai tuoreella aikajänteellä | Laskupaine voi heiketä; voi tukea positiivista fundamenttikuvaa |
| `hidden_bullish_strength > 0` | Nousevan trendin jatkuminen voi saada teknistä tukea |
| `bearish_strength > 0` | Nousun rakenne voi heiketä; varoittaa ajoitusriskistä vaikka fundamentit olisivat hyvät |
| `hidden_bearish_strength > 0` | Laskevan trendin jatkuminen voi saada teknistä vahvistusta |
| Vahva bullish strength, mutta Dow-trendi edelleen DOWN | Tekniset signaalit ovat ristiriitaiset; mahdollinen varhainen käänneyritys |
| Ei divergenssiä, mutta fundamentit vahvat | Divergenssi ei anna lisävahvistusta; päätös nojaa muihin signaaleihin |
| Useita bullish-signaaleja peräkkäin lyhyessä ajassa | Voi viitata pohjan muodostumiseen, mutta myös kohinaan; tarkista muu rakenne |

Neutraali käytännön ohje:

- bullish divergence on useammin ajoituksen tukisignaali kuin itsenäinen ostoperuste
- bearish divergence on useammin riskivaroitus kuin itsenäinen myyntipäätös

## Koneelliselle lukukerrokselle suositeltu minimisääntö

Jos divergence_dataa käytetään koneellisessa analyysissä, vähintään seuraavat säännöt kannattaa toteuttaa:

1. Käytä vain rivejä, joilla `date <= analysis_as_of_date`.
2. Erota jatkuva strength ja binääriset R2/R3-event-liput toisistaan.
3. Tarkista kattavuus vertaamalla viimeisintä divergence-riviä viimeisimpään käyttökelpoiseen hintapäivään.
4. Älä käytä `pivot2_date_r2` tai `pivot2_date_r3` suoraan signaalin käyttöpäivänä.
5. Käsittele `> 0` signaalin olemassaolona ja strengthiä erillisenä prioriteettina.
6. Rajaa myös coverage analyysipäivään, ei koko kannan uusimpaan päivään.

## Yhteenveto tulkitsijalle

- `divergence_data` sisältää rivin jokaiselle käsitellylle päivälle, ei vain signaalipäiville.
- Perusstrengthit ovat trailing-laskentaa ja turvallisia käyttää ehdolla `date <= analyysipäivä`.
- R2/R3-liput ovat vahvistettuja eventtejä, joiden käyttöpäivä on rivin `date`, ei pivotin päivä.
- Divergenssille ei ole erillistä status-taulua, joten kattavuus pitää päätellä suhteessa hintadataan.
- Bullish- ja bearish-divergenssit ovat taustasignaaleja, eivät yksinään suosituksia.