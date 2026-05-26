# TODO

## Documentation

- Finish documenting all public items and uncomment `#![deny(missing_docs)]` in `lib.rs`
- Replace `unwrap()` calls in doc examples with `?` per the [Rust API guidelines](https://rust-lang.github.io/api-guidelines/documentation.html#c-question-mark)

## API

- Variadic `and()` / `or()` on `ConditionBuilder` (currently binary only); the free functions
  `and()` / `or()` in `condition.rs` are also binary — see the `TODO: variadic` comments and the
  skipped variadic tests in `condition.rs`
- `Builder::new()` is redundant with `Builder::default()` — remove it

## Code quality

- Error strings are duplicated between source and tests (copy/paste); share them via constants
  or helper functions
- The commented-out test `list_append_list_and_name` in `update.rs` needs a solution — `Vec<i64>`
  has no `impl_value_builder!` impl, so `value(vec![1, 2, 3])` does not compile
- The `compound` test in `expression.rs` notes that attribute value aliases come out in a different
  order than the Go SDK; this is benign (DynamoDB treats them equivalently) but worth understanding

## Infrastructure

- Set up GitHub Actions CI
