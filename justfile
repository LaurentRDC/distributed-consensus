format:
    # git ls-files might not return new Haskell files, so I
    # prefer to use 'find'.
    #
    # The 'find' command starts by "don't list the directory dist-newstyle"
    # (-type d -name dist-newstyle -prune)
    # but otherwise (-o) print files with names like *.hs
    # (-type f -name ... -print)
    ormolu --mode inplace $(find . -type d -name dist-newstyle -prune -o -type f -name "*.hs" -print)

lint:
    hlint .

build target='all':
    cabal build {{target}}

test target='all':
    cabal test {{target}}

pre-commit: format lint

gen-docs:
    cabal haddock --haddock-for-hackage linearizable
    cabal haddock --haddock-for-hackage wal
    cabal haddock --haddock-for-hackage raft-consensus
    cabal haddock --haddock-for-hackage raft-consensus-wal
    # raft-consensus-distributed-process makes use of public sublibraries, for which
    # Hackage doesn't support documentation. Therefore, we must be specific
    # that we want the documentation for the main library
    cabal haddock --haddock-for-hackage raft-consensus-distributed-process:lib:raft-consensus-distributed-process

[group('example: distfs')]
distfs-servers n="5" keep-state="false":
    ./examples/distributed-filesystem/run-servers.sh {{n}} {{ if keep-state == "true" { "--keep-state" } else { "" } }}

[group('example: distfs')]
distfs-client *args:
    cabal -v0 run distfs-client -- {{args}}

[group('example: dstm')]
dstm-bank n="5":
    ./examples/distributed-stm/run-servers.sh {{n}}
