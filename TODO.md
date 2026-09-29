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
  order than the Go SDK. Root cause: Go sorts `expressionType` by its string value (`condition`,
  `filter`, `keyCondition`, `projection`, `update`), but the Rust `ExpressionType` enum derives `Ord`
  in declaration order (`Projection`, `KeyCondition`, `Condition`, ...). Reordering the enum variants
  alphabetically should make the test match Go exactly

## Go SDK v2 parity

Behavior differences:

- `NameBuilder::build_operand` returns `UnsetParameterError` for empty path segments (`foo..bar`,
  `foo.`) and `[foo]`; Go (v1 and v2) returns `InvalidParameterError` for these. Only the fully
  empty name (`name("")`) should be `UnsetParameterError`
- v2 validates list index brackets in names: mismatched (`foo[`, `foo]`, `foo]1[`), empty (`foo[]`)
  and non-numeric (`foo[a]`) indexes are `InvalidParameterError`. Rust currently accepts these and
  emits a bad expression or a bogus attribute name
- `Builder::default().build()` succeeds with an empty `Expression`; Go returns
  `UnsetParameterError("Build", "Builder")`
- Empty collections: v2 marshals a non-nil empty slice/map as an empty `L` / `M` (and empty sets as
  empty `SS`/`NS`/`BS` unless `NullEmptySets` is set); only nil slices/maps become `NULL`. The
  current `Null(true)` behavior matches v1's default (`EnableEmptyCollections: false`), not v2
- `Vec<String>` / `Vec<&str>` become `SS`, but Go marshals `[]string` as a list (`L`); a string set
  needs a `stringset` struct tag. Decide whether to keep this as a documented divergence

Missing v2 API:

- `contains()` takes any value in v2 (`Contains(name, val any)`), not just a string, so you can
  check list/set membership of numbers etc.
- `ConditionBuilder::is_set()` / `KeyConditionBuilder::is_set()`
- `NameBuilder::append_name()` and `name_no_dot_split()` (names are now stored as pre-split segments
  in v2, so `name_no_dot_split("a.b")` is a single attribute literally named `a.b`)
- `value_with_options()` / `ValueBuilderOptions` (attributevalue encoder options); probably not
  applicable without a marshaller, but worth an explicit decision

## Infrastructure

- Set up GitHub Actions CI
