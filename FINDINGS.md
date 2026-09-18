# FINDINGS.md

Ranked by impact - how much of the output each issue corrupts, and whether it
affects today's run or only a future one. All numbers below were confirmed
both with standalone Python checks against the raw data and by running the
actual pipeline in Airflow (Docker), before and after each fix.

## 1. Case-sensitive buy/sell mapping drops ~36% of all rows

**What/where:** In ingest_and_clean, the side column was built from a
dictionary that only matched four exact strings (buy, sell, BUY, SELL). Any
other capitalization in Sell_Buy (e.g. "Buy", "Sell") fell through to a
missing value and was silently dropped by dropna.

**How confirmed:** Ran the mapping against the raw CSV: 1,060 of 2,975 rows
(35.6%) ended up with a missing side value - specifically every "Buy" (555
rows) and "Sell" (505 rows) spelling. Confirmed live in Airflow: the
original code logged "Loaded 2975 raw rows" then "Wrote 1915 cleaned rows" -
exactly 1,060 rows lost, matching the standalone check precisely.

**Impact:** Every downstream column (counts, averages, VWAPs, volumes,
spread) for every hour was computed on roughly two-thirds of the real
trades, with no warning.

**Fix:** Lowercase Sell_Buy before comparing, instead of listing exact
spellings, so any capitalization is caught:

```python
df["side"] = df["Sell_Buy"].str.lower()
valid = df["side"].isin(["buy", "sell"])
logger.info("Dropping %d rows with unrecognized side value", (~valid).sum())
df = df[valid]
```

**Verified after fix:** Re-ran in Airflow - log now reads "Dropping 0 rows
with unrecognized side value." Zero silent data loss.

## 2. Naive hour extraction ignores timezone offset

**What/where:** hour was built with
df["Timestamp"].astype(str).str.slice(11, 13) - this just grabs two
characters of raw text from the timestamp and ignores the +01:00/+00:00
part at the end completely.

**How confirmed:** Built the hour two different ways from the same raw
Timestamp column: one by slicing text (the buggy way), one by properly
parsing the full timestamp including its timezone offset with
pd.to_datetime(df["Timestamp"], utc=True).dt.hour. Compared the two, row by
row. 608 of 2,975 rows landed in a different hour under the naive method
than under the correct one.

**Impact:** aggregate_hourly groups by this hour value, so 608 rows were
misbucketed - some hours contained trades that didn't actually happen then,
and were missing trades that did.

**Fix:** Parse the timestamp properly and derive the hour from a real,
timezone-aware datetime instead of a text slice (see Fix #6 below - the
same code change fixes both this and the day-blending issue at once).

## 3. _vwap function is not actually volume-weighted

**What/where:** _vwap was named and documented as "volume-weighted average
price," but its formula was group["Price"].mean() - a plain average that
ignored Volume completely.

**How confirmed:** Compared the function's output against the real VWAP
formula (sum of price times volume, divided by sum of volume) on the
buy-side data: plain average 104.77, real VWAP 99.72 - a 5.05 gap.
Confirmed live in Airflow: before the fix, buy_avg_price and buy_vwap were
identical in every single row of the output. After the fix, they genuinely
differ - e.g. hour 2024-01-01 02:00: buy_avg_price = 274.9 but
buy_vwap = 438.35.

**Impact:** Anything downstream using this column assumes real volume
weighting, since that's what the name promises - a big trade should pull
the average further than a small one, and this function didn't do that.

**Fix:**
```python
def _vwap(group: pd.DataFrame) -> float:
    return (group["Price"] * group["Volume"]).sum() / group["Volume"].sum()
```

**Known, deliberately unfixed risk:** if a group has zero rows (no trades
on one side in a given hour), this becomes 0 divided by 0 - undefined, and
pandas returns a blank (NaN). This didn't happen under the original
hour-of-day grouping (every bucket had 90+ rows), but after fixing #6
(below) to use smaller, per-calendar-hour buckets, this actually happened
once in the fixed output (hour 2024-01-03 23:00, 0 sells that hour) -
confirmed live via a warning in the Airflow logs, and blank cells in the
output. It doesn't crash anything. Chose not to build handling for it
given time; the right fix would be to mark such rows clearly (e.g. a
low_confidence column) instead of leaving a silent blank.

## 4. Missing prices and volumes filled with a single overall average

**What/where:** df["Price"].fillna(df["Price"].mean()) and the same for
Volume filled every missing value with one number worked out from the
whole dataset, no matter when the trade happened.

**How confirmed:**
- 59 rows had a missing Price; 51 rows had a missing Volume.
- Both columns are skewed by a few extreme values: Price average 91.24 vs
  typical value (median) 61.11; Volume average 1,439.82 vs typical value
  1,042.00 - pulled up by 32 rows with Price over 1,000 (up to 4,883.99).
- Tested it directly: took 100 rows with real known prices, hid each one,
  and compared two fill methods. Using the median of other rows in the
  same hour was off by 19.56 on average; using the single overall average
  was off by 31.32 - about 38% more error.
- Ran the same test on Volume: using the same-hour median barely helped
  compared to a plain overall median (1,055.60 vs 1,056.52 error), and
  Volume didn't show a real pattern by hour, day, or side either. So
  grouping by hour doesn't help for this column.
- Confirmed live in Airflow: the buggy output showed the same repeating
  decimal ending (e.g. .81532147743) showing up in unrelated hours' volume
  totals - the sign of one single fill value being reused everywhere. Gone
  after the fix.

**Impact:** 59 (Price) / 51 (Volume) rows got a filled-in value that had
nothing to do with the hour it actually happened in, feeding into every
average, VWAP, and total for those hours.

**Fix:** Different fill method per column, based on what the data actually
showed:
- Price: fill with the median of other rows in the same hour (clearly
  better, since price genuinely changes through the day).
- Volume: fill with a simple overall median, no grouping (grouping by hour
  didn't measurably help here).

## 5. Appending without checking for duplicates

**What/where:** summary.to_csv(SUMMARY_PATH, mode="a", header=not
os.path.exists(SUMMARY_PATH), index=False) always adds new rows to the end
of the file, with no check for whether a given hour's row is already
there.

**How confirmed:** Tested the exact same write logic on its own: running
it twice took a 3-row file to 6 rows. Confirmed in the real Airflow
pipeline too: running the DAG a second time with no code changes took the
73-row summary file to 146 rows, every hour duplicated with the same
values.

**Impact:** Any rerun (a retry, or someone manually running it again)
silently doubles the output. Any calculation done across the whole file
afterward (like a daily average) would be thrown off, even though each
individual row's numbers are still correct on their own.

**Fix:** Before writing, check which hours are already in the existing
summary file, and only add the ones that aren't there yet:

```python
if os.path.exists(SUMMARY_PATH):
    existing = pd.read_csv(SUMMARY_PATH)
    summary = summary[~summary["hour"].astype(str).isin(existing["hour"].astype(str))]
```

This uses the summary file itself to know what's already been saved - no
extra setup needed.

**Verified after fix:** Ran the DAG again after the fix; the row count
stayed at 73, no duplicates.

**Known limitation, not built now:** this only stops exact duplicate rows.
If a new trade for an hour that was already saved comes in later, the fix
will just skip that hour instead of updating it with the new trade.

## 6. "Hourly" summary mixes all days into one bucket per hour-of-day

**What/where:** hour only ever held a number 0-23, so aggregate_hourly's
groupby("hour") could only ever make 24 groups total, no matter how many
days the raw data covered - this happens on a single run, no rerun needed.

**How confirmed:** On the file given, a single run's "hour 5" row mixed
together Jan 1 (19 rows, 57.90 average), Jan 2 (31 rows, 54.39 average),
and Jan 3 (22 rows, 59.53 average) into one row. Also checked what happens
as more data arrives: Jan 1 alone gives an hour-5 average of 57.90, but
once Jan 2 is added to the same file, that same "hour 5" row changes to
55.72 - even though Jan 1's actual trades never changed. Confirmed in
Airflow: the original output always had exactly 24 rows no matter how many
days were in the file; after the fix, 73 rows, one per actual hour on the
calendar.

**Impact:** For intraday trading, each day's specific hour probably
matters on its own. Mixing days together hides real day-to-day price
movement, and the meaning of each row silently changes depending on how
much data happens to be in the file when it runs.

**Fix:** Group by the full date and hour together, not just hour-of-day:

```python
df["ts_parsed"] = pd.to_datetime(df["Timestamp"], utc=True)
df["hour"] = df["ts_parsed"].dt.floor("h")
```

This one change also fixes #2 (naive hour extraction), since hour now
comes from a properly read, timezone-aware timestamp.

**Considered but not built:** a better long-term fix would be to only read
new rows from the raw file each time, instead of reading the whole file
every run. But to do that, the code would need to remember which rows it
already read last time, and there's currently no way anywhere in this
pipeline to remember that. So instead, the fix above uses the summary file
itself to check what's already been saved, since that already exists and
needs no extra setup.

---

## Open question: was there an unusual price event during this period?

I define "unusual" as a price way outside the normal range - 32 of 2,975
rows have a Price above 1,000 (up to 4,883.99), while the normal range is
20-99. I checked whether these show up more on certain dates, hours, or
buy/sell sides, and found no pattern - they're spread evenly across the
whole period. I also checked whether they line up with genuinely large
trades, since a real price spike caused by a big trade should come with
unusually high volume too: it doesn't. These rows actually have slightly
lower volume than normal trades, and price and volume show no real
relationship anywhere in the dataset.

I don't think this was a real market event. Dividing the outlier prices by
exactly 100 brings 69% of them back into the normal range (e.g. 4,611.95
becomes 46.12), and doing the same check on the extreme volume values
brings all of them back to normal. That specific pattern - a clean 100x
fix that works on two separate columns, on different rows, with no shared
timing - looks more like a repeated scaling mistake somewhere upstream
than an actual price event. This is a proposal, not a settled answer - I
can't confirm where the 100x factor comes from using just this data, and
about a third of the price outliers don't fit this pattern either. I'd
suggest the team check the raw upstream feed to see where this scaling
might be coming from.
