# SFU-EVP / ChargePointDataset

Download ChargePoint history and upload the dataset to IEEE DataPort using foreground, single-run command-line tools. They do not create daemons, run background workers, or schedule recurring work.

## Setup

Use Python 3.10 or newer and an activated virtual environment:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Store configuration **outside the checkout**. From the project directory, create a private copy of the safe template:

```sh
mkdir -p "$HOME/.config/sfu-evp"
chmod 700 "$HOME/.config/sfu-evp"
(umask 077; cp -n config.example.ini "$HOME/.config/sfu-evp/config.ini")
chmod 600 "$HOME/.config/sfu-evp/config.ini"
```

The copy command preserves an existing configuration. Open `~/.config/sfu-evp/config.ini` in your local editor and fill in `api_key` and `secret` under `[ChargePoint]` and `[DataPort]`. Use your Canadian ChargePoint API credentials and IEEE DataPort upload credentials. Do not paste keys into shell commands or store the private file in `data/` or `log/`, because those directories are uploaded.

The runners search in this order: `--config PATH`, the `SFU_EVP_CONFIG` environment variable, then `~/.config/sfu-evp/config.ini`. They never automatically load a repository `config.ini`. Credentials are read by name; percent characters are treated literally. Legacy `[Parameters]` frequency settings are ignored.

Data and logs default to the checkout containing the scripts, independently of where the private configuration lives. Optionally add `root = /absolute/path/to/dataset` under `[Paths]` to choose another dataset directory; relative roots are resolved against the private configuration directory. Individual relative data/log paths are resolved against that dataset root.

### Migrating an older checkout

Move the existing `config.ini` to `~/.config/sfu-evp/config.ini` instead of copying the template over it. Preserve any existing private configuration before moving. Apply the directory/file permissions above. Add `[Paths] root` if the dataset should live outside the script directory. If Git still tracks the old file, run `git rm --cached --ignore-unmatch config.ini` and commit its removal together with `.gitignore` and `config.example.ini`. Never stage a credential-bearing replacement. The ignore rules protect accidental local config copies; `git add -f` can still bypass them.

Removing a tracked file does **not** erase earlier commits. If real keys were ever committed or published, revoke/rotate those keys with ChargePoint and IEEE DataPort/AWS as appropriate. Cleaning shared Git history requires a separate coordinated change; this migration does not rewrite history.

## Run

```sh
./download_chargepoint-data.py
./refresh_public-dataset.py --dry-run
./refresh_public-dataset.py
```

All three scripts exit after one run: status 0 means success, 1 means failure, and 130 means interrupted. Use `--help` for options. Activate the environment first so the executable scripts find the installed dependencies. They can also be invoked by absolute path from another directory. Configuration selection and dataset path resolution are described above.

The upload destination remains `s3://ieee-dataport/open/27422/11280/`. Files in `data/` and `log/` are uploaded under their basenames, preserving the original upload scope. Hidden/temporary files and directories are excluded. A dry run lists the exact files without contacting DataPort. Upload failures are reported, remaining files are attempted, and any failure produces a nonzero exit code.

### Download the published dataset for a new setup

After installing dependencies and configuring `[DataPort] api_key` and `secret` in the private configuration, run:

```sh
./download_public-dataset.py --dry-run
./download_public-dataset.py
```

The utility reads the IEEE DataPort **Access on AWS** location `s3://ieee-dataport/open/27422/11280/`. It downloads direct `*.csv` files into `data/` and direct `*.log` files into `log/`, creating those directories when needed. Other file types and nested objects are ignored. This is the current IEEE source, not Harvard Dataverse. Anonymous listing is denied by IEEE DataPort, so this utility needs the private DataPort credentials; ChargePoint credentials are not used.

| Remote file | Local destination |
| --- | --- |
| `AlarmData.log` | `log/AlarmData.log` |
| `Alarms.csv` | `data/Alarms.csv` |
| `Anomalies.csv` | `data/Anomalies.csv` |
| `Sessions.csv` | `data/Sessions.csv` |
| `SessionsData.log` | `log/SessionsData.log` |
| `Stations.csv` | `data/Stations.csv` |
| `StationsData.log` | `log/StationsData.log` |
| `upload.log` | `log/upload.log` |

For every existing destination file, the utility asks:

```text
overwrite Sessions.csv (y/n)?
```

Answer `y` to replace that file or `n` to keep it and continue. Each file receives its own prompt; invalid answers are requested again. If input is unavailable, the existing file is kept. New files download without a prompt. Downloading uses temporary files followed by atomic replacement, so an interrupted or failed transfer preserves the existing file. Individual failures are reported while remaining files are attempted. Exit status is 0 for completion (including declined overwrites), 1 for failures, or 130 for interruption.

`--dry-run` lists the remote files and destinations without downloading or prompting. `--config` and `[Paths] root` work as for the other commands; paths do not depend on the current terminal directory. Downloads preserve the published contents as-is, including any existing anomaly rule versions. The utility does not update ChargePoint history or upload anything.

## Canadian ChargePoint service

Both the WSDL and SOAP requests use Canada:

- WSDL: https://webservices-ca.chargepoint.com/cp_api_5.1.wsdl
- SOAP: https://webservices-ca.chargepoint.com/webservices/chargepoint/services/5.1

The SOAP service is explicitly bound to the Canadian endpoint, overriding any default address inside the WSDL. TLS verification remains enabled. WSDL retrieval has a 30-second timeout, SOAP calls a 120-second timeout, and temporary connection failures receive bounded retries. WSDL caching is stored at `~/.cache/SFU-EVP/wsdl.sqlite` for 24 hours. API error codes and SOAP faults are reported instead of being treated as missing data.

Endpoint reference: [ChargePoint API 5.1 guide](https://docs.chargepoint.com/ref-docs-sec/content/pdfs/4-software/api/cp_api5.1.pdf).

### Authentication troubleshooting

`InvalidSecurity: UsernameToken processing failed` means the Canadian service rejected the SOAP security token before session data was returned. It does not identify the exact credential/account problem, and retrying a larger or smaller date range will not resolve authentication.

1. Check the selected private configuration: `--config` overrides `SFU_EVP_CONFIG`, which overrides `~/.config/sfu-evp/config.ini`.
2. In the Canadian ChargePoint Network Manager portal, select the correct organization and check **Organizations > API Info**. Use the API license key and its generated API password, not your website sign-in credentials.
3. Update `[ChargePoint] api_key` and `secret` in the private file. Paste the values without surrounding quotes. Do not post them in logs, issues, or chat.
4. If the API password was regenerated, replace the old value in the private file. Regenerating a shared password may affect other integrations.
5. If the credentials match the Canadian organization and the error persists, ask ChargePoint support to verify Canadian API access and the key/password status.

The client uses WS-Security `PasswordText` over verified HTTPS, as documented by ChargePoint. Authentication faults are not retried. No automatic fallback to another regional endpoint is attempted.

## Incremental updates

Sessions use the maximum **start** timestamp, rather than the last row's end time, to resume in UTC. Each run queries through a fixed current time and revisits seven days by default; unfinished sessions extend that window back to their start. Existing session IDs are updated with the latest values. Alarm identity combines station, port, type, and timestamp, preserving simultaneous alarms at different stations or ports. Alarms are matched after sessions are updated.

```sh
./download_chargepoint-data.py --overlap-days 30
./download_chargepoint-data.py --since 2023-01-01
```

Use `--since` for a historical reconciliation or to recover records missed by the old downloader. Late records or corrections outside the overlap require a wider window or explicit date. No bounded overlap can guarantee detection of arbitrarily old changes. Previously discarded alarms must be downloaded again to recover them. With no history, downloading starts at 1970 as before.

CSV column names and identifier hashing remain compatible with the existing dataset. Missing optional API fields remain blank; stations may have any number of ports. Versioned anomaly rules use vectorized calculations and refresh the revisited sessions; see [Anomalies](#anomalies) below. Each CSV replacement is atomic; unchanged outputs retain their timestamps. The CSV files are still read into memory, and changed CSVs require a full rewrite to safely replace corrected rows. An update is not a transaction across all files: if a later stage fails, earlier completed files remain updated and the command exits unsuccessfully. Rerun the update before uploading. Run only one update at a time.

## Verification

```sh
python -m unittest discover -s tests -v
```

Tests use mocked services and temporary CSVs. They do not download private history or upload to DataPort.

## Analysis reproduction

The original analysis is available in `analysis/chargepoint_analysis.ipynb`, using `analysis/Sessions.csv`. Notebook dependencies may be installed separately from the runner dependencies.

## Repository layout

- `download_public-dataset.py`: executable command to initialize local data/log files from IEEE DataPort.
- `download_chargepoint-data.py`: executable ChargePoint history update command.
- `refresh_public-dataset.py`: executable IEEE DataPort upload command.
- `lib/`: supporting API client, configuration/storage utilities, and anomaly checks.
- `tests/`: regression tests.
- `analysis/`: analysis notebooks.

Run the commands from the project root or by absolute path. Moving support modules into `lib/` does not change configuration lookup or the dataset root.

## Anomalies

[`lib/anomalies.py`](lib/anomalies.py) detects unusual usage, possible charging failures, and inconsistent session data. Results are written to `data/Anomalies.csv`. Each row represents one triggered rule for one session; a session may have several rows. Flags are review candidates, not confirmed charger faults.

### Output columns

| Column | Meaning |
| --- | --- |
| `session_id` | Session identifier matching `Sessions.csv`. |
| `anomaly_description` | Description of the triggered rule. |
| `value` | Observed value or calculated difference, as specified below. |
| `unit` | Unit of the reported value; `raw` means the original invalid input. |
| `ver` | Rule-set version used for the result: `1` or `2`. |

### Version comparison

| Behaviour | Version 1 | Version 2 |
| --- | --- | --- |
| Long session and active charging | Original duration checks. | Same thresholds; descriptions now say “at least.” |
| Session-average power | Labelled “Charging power,” but calculated over the whole elapsed session. | Same calculation, explicitly labelled “Session-average power.” |
| Active-time average power | Not checked. | Separate average over active charging time. |
| Data quality | No explicit validation rules. | Invalid values, negative values, duration conflicts, and energy without time. |
| Charging and usage | No additional checks. | Zero-energy sessions, long noncharging occupancy, same-port overlaps, and quick restarts. |
| Historical results | Existing unversioned rows are labelled `ver=1` without recalculation. | New/revisited sessions are evaluated under version 2; all resulting flags carry `ver=2`. |

### Original rules: version 1, retained in version 2

| Version 1 description | Version 2 description | Trigger | Stored value / unit |
| --- | --- | --- | --- |
| User plugged in for longer than 24 hours | Session duration at least 24 hours | Recorded session duration >=24 hours. | Recorded duration / `hh:mm:ss`. |
| Charging power exceeds 7 kW | Session-average power exceeds 7 kW | Energy divided by elapsed hours >7 kW; elapsed time >=36 seconds. | Session-average power / `kW`. |
| User actively charging for longer than 12 hours | Active charging duration at least 12 hours | Recorded active duration >=12 hours. | Recorded active duration / `hh:mm:ss`. |

The original duration descriptions say “longer than,” but the comparisons include equality. Version 2 corrects the wording without changing the thresholds.

### Additional version 2 rules: data quality

| Anomaly description | Trigger | Stored value / unit |
| --- | --- | --- |
| Missing or invalid start timestamp | Start is missing, nonnumeric, or nonfinite. | Original start / `raw`. |
| Invalid end timestamp | A supplied end is nonnumeric or nonfinite. Blank/missing ends are allowed for unfinished sessions. | Original end / `raw`. |
| Missing or invalid energy | Energy is missing, nonnumeric, or nonfinite. | Original energy / `raw`. |
| Missing or invalid session duration | Session duration cannot be parsed. | Original duration / `raw`. |
| Missing or invalid active charging duration | Active duration cannot be parsed. | Original duration / `raw`. |
| Negative energy | Energy <0. | Energy / `kWh`. |
| Negative session duration | Session duration <0. | Signed duration / `seconds`. |
| Negative active charging duration | Active duration <0. | Signed duration / `seconds`. |
| End timestamp precedes start | Supplied end is earlier than start. | End minus start (negative) / `seconds`. |
| Session duration disagrees with timestamps by more than 60 seconds | Completed session: absolute difference between recorded session duration and elapsed time >60 seconds. | Absolute difference / `seconds`. |
| Active charging exceeds session duration by more than 60 seconds | Active duration minus session duration >60 seconds. | Excess duration / `seconds`. |
| Positive energy with zero elapsed time | Completed session: energy >0 and end equals start. | Energy / `kWh`. |
| Positive energy with zero active charging time | Energy >0 and active duration =0. | Energy / `kWh`. |

### Additional version 2 rules: charging and usage

| Anomaly description | Trigger | Stored value / unit | Interpretation |
| --- | --- | --- | --- |
| Zero energy during a session of at least 5 minutes | Completed session: energy =0 and recorded session duration >=5 minutes. | Session duration / `minutes`. | Possible unsuccessful charge; vehicle behaviour or reporting issues may also explain it. |
| Noncharging occupancy at least 12 hours | Completed session: session duration minus nonnegative active duration >=12 hours. | Noncharging duration / `hours`. | Long occupancy without current flowing; not necessarily idle time after charging finished. |
| Active-time average power exceeds 7 kW (at least 5 charging minutes) | Energy divided by active hours >7 kW; active duration >=5 minutes. | Active-time average power / `kW`. | High average power for review; does not measure instantaneous peak power. |
| Same-port session overlap exceeds 60 seconds | A valid session overlaps an earlier-starting valid session on the same station/port by >60 seconds. | Overlap duration / `seconds`. | Flags the later-starting session; investigate conflicting records or reporting delays. |
| Same-driver same-port restart within 5 minutes | Same nonmissing driver, station and port: the next valid session starts 0–300 seconds after the preceding valid session ends. | Gap between sessions / `seconds`. | Possible retry; legitimate reconnects can also trigger it. |

Elapsed time is `end_ts - start_ts`. A completed session has a supplied finite end, a finite start, and nonnegative elapsed time. Neighbour comparisons use finite timestamps and strictly positive elapsed time, excluding missing station/port values. Retry comparisons also exclude missing driver IDs and recognized legacy hashes of missing IDs.

### Historical data and incremental updates

No historical rebuild is required. Existing rows without `ver` receive `1`, preserving their other values. Each download replaces anomaly rows only for newly downloaded or revisited session IDs, using version 2. Resolved flags are removed. Untouched sessions retain their original rows and versions. Every flag from a version 2 evaluation—including the three retained rules—receives `ver=2`.

Mixed-version history is intentional. An old session without a version 2 flag has not necessarily passed the new checks. Sessions without anomalies have no row, so this CSV does not record their evaluation version. Account for these differences when comparing anomaly counts over time.

Overlap and retry checks use merged history as context but emit results only for selected sessions. An old correction can affect a neighbouring untouched session; use `--since` to revisit that period when needed. If no anomaly file exists, the downloader builds one from all available sessions using version 2. Calling `scan_anomalies` explicitly also performs a full version 2 rebuild; ordinary updates to an existing file do not.

### Interpretation and limitations

The 7 kW thresholds are review heuristics, not validated limits for every station. Current station metadata does not cover all historical ports, so station-specific limits and correlation with fault alarms remain future work. Usage, retry, overlap, and zero-energy flags alone do not establish a hardware fault. Categories in these tables are explanatory; the CSV has no separate category or severity column.

[`analysis/anomaly_audit.ipynb`](analysis/anomaly_audit.ipynb) uses version 1 to reproduce the original audit. Its exploratory candidate counts are not final version 2 counts: for example, the implemented overlap rule allows a 60-second tolerance.
