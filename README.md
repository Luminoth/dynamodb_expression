# DynamoDB Expression

Port of [Go DynamoDB Expressions](https://github.com/aws/aws-sdk-go/tree/master/service/dynamodb/expression) to Rust.

Provides builders for all DynamoDB expression types: condition, filter, key condition, projection, and update.

## Usage

Add to `Cargo.toml`:

```toml
[dependencies]
dynamodb_expression = "0.1.8"
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

# tokio_test::block_on(async {
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
    .unwrap();

let result = client.query()
    .table_name("MyTable")
    .key_condition_expression(expr.key_condition().cloned().unwrap())
    .filter_expression(expr.filter().cloned().unwrap())
    .projection_expression(expr.projection().cloned().unwrap())
    .set_expression_attribute_names(expr.names().clone())
    .set_expression_attribute_values(expr.values().clone())
    .send()
    .await
    .unwrap();
# })
```

> **Note:** Always pass both `expression_attribute_names` and `expression_attribute_values` from the built expression — all names and values are aliased automatically.

## Supported value types

| Rust type | DynamoDB type |
|-----------|---------------|
| `bool` | BOOL |
| `i64` | N |
| `f64` | N |
| `&'static str` / `String` | S |
| `Vec<&'static str>` / `Vec<String>` | SS |
| `Vec<Box<dyn ValueBuilderImpl>>` | L |
| `HashMap<String, Box<dyn ValueBuilderImpl>>` | M |
| `aws_sdk_dynamodb::types::AttributeValue` | any |

For unsigned integers or other numeric types, use `value(n as i64)` or pass an `AttributeValue` directly.
