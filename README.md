# Danish address export

Downloads danish access-addresses, unit-addresses, post-codes, roads. Doing the download the application maps them to english and then exports them as jsonline files to the specified folder.

## Build

Requires `clojure` installed on the machine.

```sh
clojure -M:uberdeps
```

# Run build

```sh
java -cp target/danish-address-export.jar clojure.main -m danish-address-export.core <destination-folder>
```
