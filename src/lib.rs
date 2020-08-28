//#![deny(warnings)]

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
impl_value_builder!(String);
impl_value_builder!(rusoto_dynamodb::AttributeValue);
impl_value_builder!(Vec<Box<dyn ValueBuilderImpl>>);
impl_value_builder!(std::collections::HashMap<String, Box<dyn ValueBuilderImpl>>);
