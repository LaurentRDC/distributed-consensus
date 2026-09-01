format:
    ormolu --mode inplace $(find . -name '*.hs')

lint:
    hlint .

build target='all':
    cabal build {{target}}

test target='all':
    cabal test {{target}}

pre-commit: format lint
