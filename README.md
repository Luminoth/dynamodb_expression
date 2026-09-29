# DynamoDB Expression

Port of [Go v2 DynamoDB Expressions](https://github.com/aws/aws-sdk-go-v2/tree/main/feature/dynamodb/expression) to Rust.

Provides builders for all DynamoDB expression types: condition, filter, key condition, projection, and update.

## Usage

Add to `Cargo.toml`:

```toml
[dependencies]
dynamodb_expression = "0.4.0"
aws-sdk-dynamodb = "1"
```

### Condition / Filter expression

```rust
use dynamodb_expression::*;

// Artist = :a
let cond = name("Artist").equal(value("No One You Know"));

// Price > :p AND Rating >= :r
let cond = name("Price").greater_than(value(10))
    .and(name("Rating").greater_than_equal(value(4)));

// attribute_exists(Thumbnail)
let cond = attribute_exists(name("Thumbnail"));
```

### Key condition expression

```rust
use dynamodb_expression::*;

// Exact match on partition key
let key_cond = key("PK").equal(value("USER#123"));

// Partition key + sort key range
let key_cond = key("PK").equal(value("USER#123"))
    .and(key("SK").begins_with("ORDER#"));
```

### Projection expression

```rust
use dynamodb_expression::*;

let proj = names_list(name("Title"), vec![name("Author"), name("Year")]);
```

### Update expression

```rust
use dynamodb_expression::*;

// SET and REMOVE in one expression
let update = set(name("Status"), value("active"))
    .set(name("UpdatedAt"), value("2024-01-01"))
    .remove(name("TempField"));
```

### Building and using with the AWS SDK

```rust
use dynamodb_expression::*;

#[tokio::main]
async fn main() {
    let shared_config = aws_config::from_env().load().await;
    let client = aws_sdk_dynamodb::Client::new(&shared_config);

    let key_cond = key("PK").equal(value("USER#123"));
    let filter = name("Active").equal(value(true));
    let proj = names_list(name("PK"), vec![name("SK"), name("Name")]);

    let expr = Builder::new()
        .with_key_condition(key_cond)
        .with_filter(filter)
        .with_projection(proj)
        .build()
        .expect("failed to build expression");

    let result = client.query()
        .table_name("MyTable")
        .key_condition_expression(expr.key_condition().cloned().expect("key condition was set"))
        .filter_expression(expr.filter().cloned().expect("filter was set"))
        .projection_expression(expr.projection().cloned().expect("projection was set"))
        .set_expression_attribute_names(expr.names().clone())
        .set_expression_attribute_values(expr.values().clone())
        .send()
        .await
        .expect("query failed");
}
```

> **Note:** Always pass both `expression_attribute_names` and `expression_attribute_values` from the built expression — all names and values are aliased automatically.

## Supported value types

How to produce each DynamoDB type with the Go SDK v2 (`expression.Value(...)`) and with this crate (`value(...)`):

| DynamoDB type | Go SDK v2 | Rust |
|---------------|-----------|------|
| S (string) | `string` | `&'static str` / `String` |
| N (number) | `int64`, `float64` (and other numeric types) | `i64`, `f64` |
| B (binary) | `[]byte` | `AttributeValue::B(Blob::new(...))` |
| BOOL | `bool` | `bool` |
| NULL | `nil` | `AttributeValue::Null(true)` |
| L (list) | `[]string`, `[]any` (any non-byte slice) | `Vec<&'static str>` / `Vec<String>`, `Vec<Box<dyn ValueBuilderImpl>>` |
| M (map) | `map[string]any` | `HashMap<String, Box<dyn ValueBuilderImpl>>` |
| SS (string set) | `&types.AttributeValueMemberSS{...}` | `AttributeValue::Ss(...)` |
| NS (number set) | `&types.AttributeValueMemberNS{...}` | `AttributeValue::Ns(...)` |
| BS (binary set) | `[][]byte` | `AttributeValue::Bs(...)` |

Any `types.AttributeValue` (Go) or `aws_sdk_dynamodb::types::AttributeValue` (Rust) is passed through unchanged, so the `AttributeValue` forms above work for every type. `Blob` is `aws_sdk_dynamodb::primitives::Blob`.

Neither SDK makes a set from a plain list: a slice or `Vec` of strings is always a list (L). In Go, a struct field tagged `dynamodbav:",stringset"` becomes a set, but only inside a struct, which marshals as M.

Empty `Vec`s and `HashMap`s produce an empty L / M, which is how Go handles empty (non-nil) slices and maps.

For unsigned integers or other numeric types in Rust, cast with `value(n as i64)` or pass `AttributeValue::N(n.to_string())`.
