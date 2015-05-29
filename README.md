# Apache Log parser plugin for Embulk

TODO: Write short description here and build.gradle file.

## Overview

* **Plugin type**: parser
* **Guess supported**: no

## Configuration

- **property1**: description (string, required)
- **property2**: description (integer, default: default-value)

## Example

```yaml
in:
  type: any file input plugin type
  parser:
    type: apache-log
    property1: example1
    property2: example2
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
