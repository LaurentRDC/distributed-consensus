format:
    ormolu --mode inplace $(find . -name '*.hs')

lint:
    hlint .

build target='all':
    cabal build {{target}}

test target='all':
    cabal test {{target}}

pre-commit: format lint

[group('example: distfs')]
distfs-servers n="5" keep-state="false":
    ./examples/distributed-filesystem/run-servers.sh {{n}} {{ if keep-state == "true" { "--keep-state" } else { "" } }}

[group('example: distfs')]
distfs-client *args:
    cabal run distfs-client -- {{args}}