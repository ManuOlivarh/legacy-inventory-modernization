# The legacy ERP

A reference for reading the system we are not allowed to change. Everything
here is the "before" side of the anti-corruption layer.

## Tables

| Table | Holds | Primary key |
|---|---|---|
| `INV_ART` | Articles (products) | `ART_COD` |
| `INV_BOD` | Locations: central warehouse and stores | `BOD_COD` |
| `INV_STK` | Stock per article and location | `ART_COD`, `BOD_COD` |
| `INV_MOV` | Stock movements, append-only | `MOV_NUM` |

## Column glossary

The abbreviations are Spanish, as in many Latin American ERPs of that era.

| Column | Means | Note |
|---|---|---|
| `ART_COD` | Article code | `CHAR(12)`, zero-padded |
| `ART_DES` | Description | Cryptic: `CAM MC AZ T-M ALG` is a blue short-sleeve cotton shirt, size M |
| `ART_CAT` | Category | `ROP` clothing, `CAL` footwear, `HOG` home, `ELE` electrical, `LIM` cleaning, `HIG` hygiene, `ABA` groceries |
| `ART_UM` | Unit of measure | `UND` unit, `PAR` pair, `PQT` pack, `BOL` bag |
| `ART_EST` | Status | `A` active, `I` inactive |
| `BOD_TIP` | Location type | `C` central warehouse, `T` store |
| `STK_EXI` | On hand | `DECIMAL(12,2)` even for countable units, and it may go negative |
| `STK_RES` | Reserved | Today always 0; reservations are the strangler target |
| `MOV_TIP` | Movement type | See below |
| `MOV_CAN` | Quantity | Always positive; the direction lives in `MOV_TIP` |
| `FEC_ALT` / `FEC_ULT` / `MOV_FEC` | Created / last updated / movement timestamp | Local time, no timezone |

## Movement types

| Code | Meaning | Effect on stock |
|---|---|---|
| `VT` | Sale | − |
| `DV` | Return | + |
| `RC` | Receipt from the warehouse or a supplier | + |
| `AP` | Stock-count adjustment up | + |
| `AN` | Stock-count adjustment down | − |

## Traps this schema sets for consumers

1. **The sign is not in the quantity.** A consumer that sums `MOV_CAN` gets a
   number that means nothing. `MOV_TIP` decides direction.
2. **Stock can go negative.** `SP_INV_MOVIMIENTO` does not check availability,
   which is exactly how the overselling in the business case happens.
3. **Decimals for whole units.** `decimal.handling.mode=string` in the
   connector keeps the exact value instead of a lossy double.
4. **Fixed-width codes.** `CHAR` columns arrive padded; the translator trims.
5. **No timezone.** The ERP server runs in local time and `DATETIME` stores no
   zone. Debezium sends those values as milliseconds since epoch *read as if
   they were UTC*, so the wall-clock reading is right and the instant is wrong
   by the offset. The translator reinterprets them in `America/Guatemala`.
   `DATETIME2` columns would arrive in microseconds instead, which is the kind
   of detail that silently shifts data by a factor of a thousand.

## Write path

The legacy application only ever calls `dbo.SP_INV_MOVIMIENTO`, which inserts
into `INV_MOV` and updates `INV_STK` in one transaction. CDC therefore sees a
movement and its resulting stock row within the same transaction.
