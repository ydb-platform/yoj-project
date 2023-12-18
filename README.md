🦔 YDB ORM for Java (YOJ)
=========================

YDB ORM for Java (YOJ) is a lightweight ORM for immutable entities.

YOJ integrates well with YDB, and we also have an in-memory repository implementation with YDB-like semantics for 
lightning-fast persistence tests.

If you wish to contribute to YOJ, see the [Notice to external contributors](CONTRIBUTING.md).

----

**🦔 YOJ has the following modules:**
- `databind`: Core data-binding logic used to convert between Java objects and database rows (or anything representable
by a Java `Map`, really).
- `repository`: Core abstractions and APIs for entities, repositories, transactions etc. Entity API is designed to be
minimally intrusive, so that your domain objects (with all the juicy business logic!) can easily become entities.
- `repository-ydb-v2`: Repository API implementation for YDB. Uses YDB SDK v2.x. **Recommended.**
- `repository-inmemory`: In-Memory Repository API implementation using persistent data structures from Eclipse 
Collections. Has YDB-like semantics for data modification, to easily and quickly test your business logic without 
spinning containers or accessing a real YDB installation. **Highly recommended.**
- `repository-ydb-v1`: Repository API implementation for YDB. Uses legacy YDB SDK v1.x. **Not recommended** for new
  projects.
- `repository-ydb-common`: Common Logic for all YDB Repository implementations, regardless of the YDB SDK version used.
- `repository-test`: Basic tests which all Repository implementations must pass.
- `util`: Utility classes used in YOJ implementation.