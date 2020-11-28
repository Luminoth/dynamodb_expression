//! Port of [Go DynamoDB Expressions](https://github.com/aws/aws-sdk-go/tree/master/service/dynamodb/expression) to Rust
//!
//! dynamodb_expression provides types and functions to create Amazon DynamoDB
//! Expression strings, ExpressionAttributeNames maps, and ExpressionAttributeValues
//! maps.
//!
//! # Usage
//!
//! Represents the various DynamoDB Expressions as structs named
//! accordingly. For example, ConditionBuilder represents a DynamoDB Condition
//! Expression, an UpdateBuilder represents a DynamoDB Update Expression, and so on.
//! The following example shows a sample ConditionExpression and how to build an
//! equilvalent ConditionBuilder
//!
//! ```
//! use dynamodb_expression::*;
//!
//! // Let :a be an ExpressionAttributeValue representing the string "No One You
//! // Know"
//! let cond_expr = "Artist = :a";
//! let cond_builder = name("Artist").equal(value("No One You Know"));
//! ```
//!
//! In order to retrieve the formatted DynamoDB Expression strings, call the getter
//! methods on the Expression struct. To create the Expression struct, call the
//! build() method on the Builder struct. Because some input structs, such as
//! QueryInput, can have multiple DynamoDB Expressions, multiple structs
//! representing various DynamoDB Expressions can be added to the Builder struct.
//! The following example shows a generic usage of the whole package.
//!
//! ```
//! use dynamodb_expression::*;
//!
//! let filt = name("Artist").equal(value("No One You Know"));
//! let proj = names_list(name("SongTitle"), vec![name("AlbumTitle")]);
//! let expr = Builder::new().with_filter(filt).with_projection(proj).build().unwrap();

//! let input = rusoto_dynamodb::ScanInput{
//!   expression_attribute_names: expr.names().clone(),
//!   expression_attribute_values: expr.values().clone(),
//!   filter_expression: expr.filter().cloned(),
//!   projection_expression: expr.projection().cloned(),
//!   table_name: "Music".to_owned(),
//!   ..Default::default()
//! };
//! ```
//!
//! The expression_attribute_names and expression_attribute_values member of the input
//! struct must always be assigned when using the Expression struct because all item
//! attribute names and values are aliased. That means that if the
//! expression_attribute_names and expression_attribute_values member is not assigned
//! with the corresponding names() and values() methods, the DynamoDB operation will
//! run into a logic error.

#![doc(html_root_url = "https://docs.rs/dynamodb_expression/0.1.2")]
#![deny(warnings)]

mod condition;
pub mod error;
mod expression;
mod key_condition;
mod operand;
mod projection;
mod update;

pub use condition::*;
pub use expression::*;
pub use key_condition::*;
pub use operand::*;
pub use projection::*;
pub use update::*;

macro_rules! impl_value_builder {
    ($type:ty) => {
        impl $crate::operand::OperandBuilder for $crate::operand::ValueBuilder<$type> {
            fn build_operand(&self) -> anyhow::Result<$crate::operand::Operand> {
                let expr = self.attribute_value();

                let node = $crate::expression::ExpressionNode::from_values(vec![expr], "$v");
                Ok(Operand::new(node))
            }
        }

        impl $crate::operand::PlusBuilder for $crate::operand::ValueBuilder<$type> {}
        impl $crate::operand::MinusBuilder for $crate::operand::ValueBuilder<$type> {}
        impl $crate::operand::ListAppendBuilder for $crate::operand::ValueBuilder<$type> {}

        impl $crate::condition::EqualBuilder for $crate::operand::ValueBuilder<$type> {}
        impl $crate::condition::NotEqualBuilder for $crate::operand::ValueBuilder<$type> {}
        impl $crate::condition::LessThanBuilder for $crate::operand::ValueBuilder<$type> {}
        impl $crate::condition::LessThanEqualBuilder for $crate::operand::ValueBuilder<$type> {}
        impl $crate::condition::GreaterThanBuilder for $crate::operand::ValueBuilder<$type> {}
        impl $crate::condition::GreaterThanEqualBuilder for $crate::operand::ValueBuilder<$type> {}
        impl $crate::condition::BetweenBuilder for $crate::operand::ValueBuilder<$type> {}
        impl $crate::condition::InBuilder for $crate::operand::ValueBuilder<$type> {}
    };
}

impl_value_builder!(bool);
impl_value_builder!(i64);
impl_value_builder!(f64);
impl_value_builder!(&'static str);
impl_value_builder!(Vec<&'static str>);
impl_value_builder!(String);
impl_value_builder!(Vec<String>);
impl_value_builder!(rusoto_dynamodb::AttributeValue);
impl_value_builder!(Vec<Box<dyn ValueBuilderImpl>>);
impl_value_builder!(std::collections::HashMap<String, Box<dyn ValueBuilderImpl>>);
