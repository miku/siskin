# ISIL attachments improvements

* 2020-02-07

The tree based filterconfig is flexible, but a bit hard to generate, as can be
seen in
[amsl.py](https://github.com/ubleipzig/siskin/blob/68105131bee8717a4aecc601a9a3ab0fd872fe1e/siskin/sources/amsl.py).
The [span-tag](https://github.com/ubleipzig/span/blob/master/cmd/span-tag/main.go) tool has moderate [test
coverage](https://github.com/ubleipzig/span/blob/5b54e6eec741746d69f2f7e1f58b403f9648df38/licensing/entry_test.go#L148-L262)
for various cases (which catches most errors), but the config generation has not.

Approach: Create small, testable API for generating these config files. Test
config generation. Replace and reduce code in
[amsl.py](https://github.com/ubleipzig/siskin/blob/68105131bee8717a4aecc601a9a3ab0fd872fe1e/siskin/sources/amsl.py).

In a next step, contemplate switching to a tabular configuration format.
