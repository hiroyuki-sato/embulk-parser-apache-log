# Apache Log parser plugin for Embulk

Embulk parser plugin for apache log (common, combined)

## Overview

* **Plugin type**: parser
* **Guess supported**: no

## Configuration

- **format**: log format(common,combined) (string, default: common)

## Example

```yaml
in:
  type: any file input plugin type
  parser:
    type: apache-log
    format: common
```

(If guess supported) you don't have to write `parser:` section in the configuration file. After writing `in:` section, you can let embulk guess `parser:` section using this command:

```
$ embulk install embulk-parser-apache-log
$ embulk guess -g apache-log config.yml -o guessed.yml
```

## Build

```
$ ./gradlew gem
```
