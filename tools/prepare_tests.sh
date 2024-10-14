cd $(dirname $0)/../
go run ./cmd --mode=unpack --mapping ./tests/data/mappings/logging.yaml --unpack-file ./tests/data/complicated_1/source._docs
mv -f ./tests/data/complicated_1/source._docs.unpacked ./tests/data/complicated_1/raw-docs.json
cd prepare || exit
go run ./