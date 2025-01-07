# cassandra-aviary

Tool for selecting and maintaining a subset for Cassandra data for canary testing

## Abstract

Canary testing is a technique to test new software versions in production by exposing a subset of users to the new version. When using Apache Cassandra as database, the distribution of data within the Cassandra cluster might worth considering. This tool connects to a Cassandra cluster, queries its topology and schema metadata and selects canaries based from all keyspaces and tables for all token ranges. Conducting tests on the canary set acquired that way will guarantee that the whole Cassandra cluster is covered.

## Concepts

In the scope of this tool a canary is considered a record consisting of mandatory field `Origin` and `PrimaryKey`. The `Origin` field contains the name of the table the canary was selected from. The `PrimaryKey` field contains the primary key of the canary record. The primary key is represented as a map of column names to values.

Optional fields of a canary are reserved and may be add in future versions of the tool.

## Installation

### Prerequisites

The tool needs Java-21 or later.

### Installation

Download and extract any of the distribution bundles from the [releases page](https://github.com/rtib/cassandra-aviary/releases).

## Configuration

Configuration is done via TypeSafe Config. Place a file at `etc/application.conf` to override the defaults. Currently, the only topic covered by the configuration is the Cassandra connection. See the [DataStax Java Driver](https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/configuration/) for more details.

### Cassandra SSL connection

Setup a truststore for the client using:

```sh
keytool -import -alias CAroot -file ca.crt -keystore etc/truststore.jks
```

Add the following snippet to your configuration:

```hocon
datastax-java-driver {
  advanced {
    ssl-engine-factory {
      class = DefaultSslEngineFactory
      truststore-path = etc/truststore.jks
      truststore-password = changeit
    }
  }
}
```

## Usage

The tool is invoked via the vendored wrapper script `bin/aviary` from the distribution package. This provides a simple command line interface allowing to select, list and verify canaries.

### Connecting to Cassandra

By default, the tool is connecting to a Cassandra cluster running on `localhost` using the default port `9042`. You may add `--contact-point=<host>` options to the command line to connect to a different cluster. Using `--auth-user=<username>` and `--auth-password` allows to authenticate against the cluster. In some use-cases `--local-dc=<DC>` and `--port=<port>` might be useful.
These options are available for commands connecting to Cassandra.

### Selecting canaries

Having the wrapper script in your `PATH`, you can select canaries using:

```sh
% aviary select
test.test: 8/16
test.foo: 6/16
foo.multitest: 5/16
```

This will select canaries for all keyspaces and tables in the Cassandra cluster. The output shows the number of selected canaries and the total number of token ranges for each table.

By default, the selector queries topology and schema metadata of the connected Cassandra cluster and iterates over all keyspaces and tables selecting a single canary from each token range. However, the selector interface allows to customize the selection process. Using the `--selector=<SelectorClass>` option allows to specify a custom selector class. The class must implement the `io.github.rtib.cassandra.aviary.selector.ICanarySelector` interface. The default selector is `io.github.rtib.cassandra.aviary.selector.RangeSelector`.

The set of selected canary entries is stored in `avaiary.json` file in the current directory. Output location might be changed using `-o <file>` options. The file is overwritten on each invocation of the selector.

### Listing canaries

The `list` command allows to list the canaries stored in the `avaiary.json` file:

```sh
% aviary list
Canary{Origin=foo.multitest, PrimaryKey={type=test, name=a5, entry=1}}
Canary{Origin=foo.multitest, PrimaryKey={type=test, name=a1, entry=1}}
Canary{Origin=foo.multitest, PrimaryKey={type=test, name=a4, entry=1}}
Canary{Origin=test.test, PrimaryKey={id=5}}
Canary{Origin=foo.multitest, PrimaryKey={type=test, name=a2, entry=1}}
Canary{Origin=test.test, PrimaryKey={id=15}}
Canary{Origin=test.test, PrimaryKey={id=16}}
Canary{Origin=test.test, PrimaryKey={id=17}}
Canary{Origin=test.test, PrimaryKey={id=7}}
Canary{Origin=test.test, PrimaryKey={id=10}}
Canary{Origin=test.foo, PrimaryKey={bar=a7}}
Canary{Origin=test.foo, PrimaryKey={bar=a6}}
Canary{Origin=test.foo, PrimaryKey={bar=a8}}
Canary{Origin=test.foo, PrimaryKey={bar=a2}}
Canary{Origin=test.foo, PrimaryKey={bar=a1}}
Canary{Origin=test.foo, PrimaryKey={bar=a5}}
Canary{Origin=foo.multitest, PrimaryKey={type=test, name=a3, entry=1}}
Canary{Origin=test.test, PrimaryKey={id=9}}
Canary{Origin=test.test, PrimaryKey={id=11}}
```

### Verifying canaries

The `verify` command allows to verify the canaries stored in the `avaiary.json` file:

```sh
% aviary verify
test.test: 8/8
test.foo: 6/6
foo.multitest: 5/5
```

This will verify that the canaries are still present in the Cassandra cluster. The output shows the number of verified canaries and the total number of canaries for each table.
