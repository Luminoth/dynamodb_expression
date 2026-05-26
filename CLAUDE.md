# CLAUDE.md

## Project goal

This is a Rust port of the [AWS SDK for Go DynamoDB Expression Builder](https://github.com/aws/aws-sdk-go/tree/master/service/dynamodb/expression).

**Fidelity to the Go source is the primary constraint.** When in doubt about naming, parameter order, behavior, or doc text, check the Go source first. The Rust API should feel idiomatic Rust while staying as close to the Go design as possible. Divergences should be intentional and documented.

Go source files map directly to Rust modules:

| Go file | Rust module |
|---------|-------------|
| `expression.go` | `src/expression.rs` |
| `condition.go` | `src/condition.rs` |
| `key_condition.go` | `src/key_condition.rs` |
| `operand.go` | `src/operand.rs` |
| `projection.go` | `src/projection.rs` |
| `update.go` | `src/update.rs` |
| `error.go` | `src/error.rs` |

## How expression building works

The pipeline has three stages:

1. **Builder structs** (`NameBuilder`, `ValueBuilder<T>`, `ConditionBuilder`, etc.) accumulate intent without allocating any DynamoDB-facing data.

2. **`build_tree()`** (via the `TreeBuilder` trait) converts a builder into an `ExpressionNode` tree. Each node stores:
   - `names: Vec<String>` — raw attribute name segments
   - `values: Vec<AttributeValue>` — raw attribute values
   - `children: Vec<ExpressionNode>` — sub-expressions
   - `fmt_expression: String` — a template using escape sequences `$n` (name), `$v` (value), `$c` (child)

3. **`build_expression_string()`** walks the node tree and resolves escape sequences against an `AliasList`, producing the final expression string and populating `ExpressionAttributeNames` / `ExpressionAttributeValues`.

`AliasList` deduplicates **names** (same attribute name reuses the same `#N` alias) but does **not** deduplicate values (each value gets a fresh `:N` alias). This matches Go SDK behavior. A known side effect: the compound expression test in `expression.rs` produces different value alias numbering than the Go SDK, but DynamoDB evaluates both identically.

## Key design patterns

**Everything is boxed.** Builder functions return `Box<T>` (e.g., `name()` returns `Box<NameBuilder>`). This mirrors Go's interface-based design and enables trait objects throughout. Clippy warnings about boxed locals are suppressed with `#[allow(clippy::boxed_local)]` where the API requires it.

**`ValueBuilder<T>` uses a type parameter as a tag.** The concrete type `T` encodes the Rust type being stored. `impl_value_builder!(T)` in `lib.rs` bulk-implements `OperandBuilder` and all condition/arithmetic traits for each supported `T`. Adding a new value type requires adding an `impl ValueBuilderImpl for ValueBuilder<NewType>` block in `operand.rs` and a corresponding `impl_value_builder!(NewType)` call in `lib.rs`.

**`ValueBuilderImpl` is a marker + object-safe supertrait.** It exists to bridge `ValueBuilder<T>` into `Box<dyn ValueBuilderImpl>` for the list/map types (`Vec<Box<dyn ValueBuilderImpl>>`, `HashMap<String, Box<dyn ValueBuilderImpl>>`).

**`OperationMode` / `ConditionMode` / `KeyConditionMode` enums drive dispatch.** Each mode corresponds to a DynamoDB expression construct. `Unset` is the `Default` and triggers `UnsetParameterError` when `build_tree()` is called on an uninitialized builder. `Invalid` (used only in `KeyConditionMode`) encodes a structurally impossible key condition detected at construction time.

**`ExpressionType` ordering is deterministic.** `build_child_trees()` sorts expression types before building so that `AliasList` alias numbering is stable across runs. This matters for tests that assert on specific alias values like `#0`, `:1`.

## Error handling

Two error variants, both in `ExpressionError`:

- `InvalidParameterError(function_name, parameter_type)` — parameter is present but syntactically wrong (e.g., `name("foo..bar")`)
- `UnsetParameterError(function_name, parameter_type)` — parameter is missing / builder was default-constructed

All public functions return `anyhow::Result<T>`. Tests downcast with `.map_err(|e| e.downcast::<ExpressionError>().unwrap()).unwrap_err()` to assert on specific error variants. Error string literals are duplicated between source and tests (known issue in TODO.md).

## Testing conventions

- Unit tests live in `#[cfg(test)]` blocks at the bottom of each module file.
- Doc examples use `# tokio_test::block_on(async { ... })` for the async AWS SDK calls; the `#` hides the boilerplate from rendered docs.
- Tests assert on `ExpressionNode` structures (intermediate representation) rather than final strings where possible — this makes failures easier to diagnose.
- The `hashmap!` macro in `expression.rs` tests is local to that test module; it is not part of the public API.

## Parameter ordering to watch

`between` / `key_between` take `(lower, upper)` in that order — lower bound first, upper bound second. This matches the Go SDK (`Between(op, lower, upper)` / `KeyBetween(key, lower, upper)`). The DynamoDB expression produced is `operand BETWEEN lower AND upper`.

## Known intentional divergences from Go

- `Builder::new()` exists alongside `Builder::default()` (redundant; Go has no `Default` trait).
- `and()` / `or()` are binary rather than variadic (Go accepts `...ConditionBuilder`); see TODO.md.
- Empty `Vec` / `HashMap` values produce `AttributeValue::Null(true)` rather than an error, matching observed Go behavior.
- `value()` accepts `&'static str` rather than any `&str`; use `String` / `.to_owned()` for runtime strings.
