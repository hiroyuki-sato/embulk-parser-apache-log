# Apache Log parser plugin for Embulk

Embulk parser plugin for apache log (common, combined)

## Overview

* **Plugin type**: parser
* **Guess supported**: no

## Configuration

- **format**: log format(common,combined) (string, default: combined)

## Example

```yaml
in:
  type: any file input plugin type
  parser:
    type: apache-log
    format: common
```

```
$ embulk gem install embulk-parser-apache-log
```

## Build

```
$ ./gradlew gem
```
