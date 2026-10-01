# seq-db `fraction` Helper Tool

Decodes the content of a sealed seq-db fraction into newline-delimited JSON or seals logs into a fraction.

Works with the current format (split files `.info/.sdocs/.tokens/.offsets/.ids/.lids`)
and with the legacy format (a single `.index` file).

## Usage

```bash
# To decode
go run ./cmd/fraction decode <frac>

# To seal
go run ./cmd/fraction seal <frac>
```

where `<frac>` is the base file name of the fraction without suffixes,
e.g. `./data/store/frac_000123`.

## Decoding

The decoded content is streamed to stdout
as ndjson: one self-sufficient JSON record per line, tagged with a `kind`
field. The output is pipeline-friendly, e.g. for jq aggregations:

```bash
# top tokens by frequency
go run ./cmd/fraction decode --only=token <frac> | jq -s 'sort_by(-.freq)[:20]'
# average document length
go run ./cmd/fraction decode --only=doc <frac> | jq -s '[.[].doc | length] | add/length'
```

### Flags

`--only=<sections>` — decode only the listed sections, comma-separated.
Available sections: `info,doc,id,token,offsets` (same as the record kinds).

```bash
go run ./cmd/fraction decode --only=info,doc <frac>
```

Sections not listed in `--only` are not decoded at all. When `tokens` is
requested alone, token records are written without the `lids` arrays
(postings) — a lighter output when only the token dictionary is needed.

### Output records

| `kind`     | Content                                                                     |
|------------|-----------------------------------------------------------------------------|
| `info`     | fraction metadata (`common.Info` fields, flattened)                         |
| `doc`      | document text with its LID; the system document (lid 0) is not included     |
| `id`       | every fraction ID: LID, MID, RID and the document position (DocPos)          |
| `token`    | inverted index: TID, field, token, frequency and the LID list (postings)     |
| `offsets`  | doc block offsets in the documents file, single record                      |

### Example

Records look like this:

```json
{"kind":"info","name":"frac_000001","docs_total":3,"index_on_disk":888,...}
{"kind":"doc","lid":1,"doc":"{\"level\":\"error\",\"service\":\"api\",\"message\":\"db timeout\"}"}
{"kind":"id","lid":1,"mid":1704103202000000000,"rid":6530305922915827712,"pos":5}
{"kind":"token","tid":5,"field":"level","token":"error","freq":1,"lids":[1]}
{"kind":"offsets","values":[0,132,264]}
```


## Sealing

Reads JSON documents from stdin (one per line) and seals them into fraction files
at `<frac>`. Useful for producing test fractions and local debugging.

```bash
cat docs.jsonl | go run ./cmd/fraction seal <frac>
```

### Flags

`--mapping=<file>` — path to a YAML mapping for `seal` (format as in
[quickstart/mappings.yaml](../../quickstart/mappings.yaml)). Without the
flag a built-in default mapping is used (`level`, `service` — keyword;
`message` — text).

## Limitations

- The fraction must be complete: all index files have to exist, even if
  `--only` requests a subset of the sections.
- Skip masks of deleted documents are ignored: the output contains every
  document of the fraction, including "deleted" ones.
