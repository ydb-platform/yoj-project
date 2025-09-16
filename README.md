🦔 YDB ORM for Java (YOJ)
=========================
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/yoj-project/blob/main/LICENSE)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Frepo1.maven.org%2Fmaven2%2Ftech%2Fydb%2Fyoj%2Fyoj-parent%2Fmaven-metadata.xml)](https://mvnrepository.com/artifact/tech.ydb.yoj/yoj-parent)
[![Build](https://img.shields.io/github/actions/workflow/status/ydb-platform/yoj-project/build.yaml?branch=main)](https://github.com/ydb-platform/yoj-project/actions/workflows/build.yaml)

**YDB ORM for Java (YOJ)** is a lightweight ORM for immutable entities.

YOJ integrates well with YDB, and it also has an in-memory repository implementation with YDB-like semantics for 
lightning-fast persistence tests.

YOJ is licensed under [Apache License, Version 2.0](LICENSE).

If you wish to contribute to YOJ, see the [Notice to external contributors](CONTRIBUTING.md), and follow the [guidelines](GUIDELINES.md).

----
**To use YOJ in your project**, just add YOJ BOM (Bill of Materials) to your Maven `<dependencies>`:
```xml
<dependency>
    <groupId>tech.ydb.yoj</groupId>
    <artifactId>yoj-bom</artifactId>
    <version>2.6.36</version>
    <type>pom</type>
    <scope>import</scope>
</dependency>
```
Then depend on just the modules you need, specifying only `groupId`=`tech.ydb.yoj` and `artifactId`=`yoj-<module>` (see `<module>` names below).

**🦔 YOJ consists of the following modules:**
- `databind`: Core data-binding logic used to convert between Java objects and database rows (or anything representable
by a Java `Map`, really).
- `repository`: Core abstractions and APIs for entities, repositories, transactions etc. Entity API is designed to be
minimally intrusive, so that your domain objects (with all the juicy business logic!) can easily become entities.
- `repository-ydb-v2`: Repository API implementation for YDB. Uses YDB SDK v2.x. **Recommended.**
- `repository-inmemory`: In-Memory Repository API implementation using persistent data structures from Eclipse 
Collections. Has YDB-like semantics for data modification, to easily and quickly test your business logic without 
spinning containers or accessing a real YDB installation. **Highly recommended.**
- `repository-ydb-common`: Common Logic for all YDB Repository implementations, regardless of the YDB SDK version used.
- `repository-test`: Basic tests which all Repository implementations must pass.
- `json-jackson-v2`: Support for JSON serialization and deserialization of entity fields, using Jackson 2.x.
- `aspect`: AspectJ aspect and `@YojTransactional` annotation for usage with AspectJ and Spring AOP. Allows a Spring `@Transactional`-like experience for your methods that need to initiate or continue a YDB transaction.
- `ext-meta-generator`: Annotation processor that generates field paths for each of your `Entity` fields, to be used with `TableQueryBuilder` (`Table.query()` DSL) and `YqlPredicate`.
- `util`: Utility classes used in YOJ implementation.
