# gold

`gold` is a Go library that provides simple wrappers for common Google Cloud services.

Currently supports:
- [Cloud PubSub](pubsub/) — publish and subscribe to topics
- [Cloud Memorystore (Redis)](redis/) — caching, locking, and distributed locks
- [Cloud Storage](storage/) — upload, download, and manage objects

## Installation

Each service is a separate Go module. Install only what you need:

```
go get github.com/louvri/gold/pubsub
go get github.com/louvri/gold/redis
go get github.com/louvri/gold/storage
```

Minimum Go: `pubsub` and `storage` need 1.26, `redis` needs 1.25.

See each module's README for usage details.

## Releasing

Merging to `main` tags a release, `<module>/vX.Y.Z`, of every module the merge changed. Each change to the module since its last tag asks for a level, and the release takes the highest:

- A `Release-As: major|minor|patch` trailer on its own line sets the level of the change it is on (the key is case-insensitive). A change marked `Release-As: skip` never triggers a release by itself; it ships with the module's next change, at no lower a level than its own markers need. On a merge into `main`, the skip covers everything the merge brought in.
- Otherwise a breaking change (a `type!:` subject or a `BREAKING CHANGE:` footer) is a minor bump below v1.0.0 and a major one from then on; from v1.0.0 a `feat:` subject is a minor bump; everything else is a patch.
- A squash merge is one change: the `* subject` lines of its body count as subjects, and a `Release-As:` trailer anywhere in it applies to the whole pull request. Pull requests are squashed, so each one touching a module is a single change to it.
- A merge commit counts on its own subject and body, and one that changes the module itself (a hand-resolved conflict) asks for at least a patch.
- A module whose code is unchanged since its tag is not released, whatever its commits ask for. Once something else changes, a reverted change still counts: revert lines are not trusted, and a bump too large is the safe mistake.
- A module's first release is v0.1.0. From v2 on, its `go.mod` must declare the matching `/vN` module path, or the release is refused; if that major bump was not intended, tag the current `main` commit by hand with the version you want and later runs start from it (a tag on a commit that is not on `main` is ignored as a base, though its version number is still skipped past - so a `v1.x` tag pushed by hand off `main` moves a module past 1.0, after which a `feat:` is a minor bump and a breaking change a major one, refused until `go.mod` declares the `/v2` path).

The rules are pinned by `.github/scripts/next-version_test.sh` (needs git 2.38 or later).
