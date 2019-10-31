# Apache Log parser plugin for Embulk

Embulk parser plugin for apache log (common, combined)

## Overview

* **Plugin type**: parser
* **Guess supported**: no

## Configuration

- **format**: log format(common,combined) (string, default: combined)
- **stop_on_invalid_record**: ignore invalid log entries (true, false) (boolean, default: true)

## Example

```yaml
in:
  type: any file input plugin type
  parser:
    type: apache-log
    format: common
    stop_on_invalid_record: true
```

```
$ embulk gem install embulk-parser-apache-log
```

## Build

```
$ ./gradlew gem
```
