//! Ported from [operand.go](https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/operand.go)

use std::collections::HashMap;

use anyhow::bail;
use aws_sdk_dynamodb::model::AttributeValue;
use derivative::*;

use crate::{error::ExpressionError, ExpressionNode};

macro_rules! into_operand_builder {
    () => {
        fn into_operand_builder(self: Box<Self>) -> Box<dyn OperandBuilder> {
            self
        }
    };
}

#[derive(Debug, Clone)]
pub struct Operand {
    pub(crate) expression_node: ExpressionNode,
}

impl Operand {
    pub(crate) fn new(expression_node: ExpressionNode) -> Self {
        Self { expression_node }
    }
}

pub trait OperandBuilder: Send {
    fn build_operand(&self) -> anyhow::Result<Operand>;
}

// marker trait for working with generic ValueBuilders
pub trait ValueBuilderImpl: OperandBuilder {
    fn attribute_value(&self) -> AttributeValue;

    fn into_operand_builder(self: Box<Self>) -> Box<dyn OperandBuilder>;
}

#[derive(Debug, Clone)]
pub struct ValueBuilder<T> {
    value: T,
}

impl<T> ValueBuilder<T> {}

impl ValueBuilderImpl for ValueBuilder<bool> {
    fn attribute_value(&self) -> AttributeValue {
        AttributeValue::Bool(self.value)
    }

    into_operand_builder!();
}

impl ValueBuilderImpl for ValueBuilder<i64> {
    fn attribute_value(&self) -> AttributeValue {
        AttributeValue::N(self.value.to_string())
    }

    into_operand_builder!();
}

impl ValueBuilderImpl for ValueBuilder<f64> {
    fn attribute_value(&self) -> AttributeValue {
        AttributeValue::N(self.value.to_string())
    }

    into_operand_builder!();
}

impl ValueBuilderImpl for ValueBuilder<&'static str> {
    fn attribute_value(&self) -> AttributeValue {
        AttributeValue::S(self.value.to_owned())
    }

    into_operand_builder!();
}

impl ValueBuilderImpl for ValueBuilder<Vec<&'static str>> {
    fn attribute_value(&self) -> AttributeValue {
        if self.value.is_empty() {
            return AttributeValue::Null(true);
        }

        AttributeValue::Ss(self.value.iter().map(|&x| x.to_owned()).collect())
    }

    into_operand_builder!();
}

impl ValueBuilderImpl for ValueBuilder<String> {
    fn attribute_value(&self) -> AttributeValue {
        AttributeValue::S(self.value.clone())
    }

    into_operand_builder!();
}

impl ValueBuilderImpl for ValueBuilder<Vec<String>> {
    fn attribute_value(&self) -> AttributeValue {
        if self.value.is_empty() {
            return AttributeValue::Null(true);
        }

        AttributeValue::Ss(self.value.clone())
    }

    into_operand_builder!();
}

impl ValueBuilderImpl for ValueBuilder<AttributeValue> {
    fn attribute_value(&self) -> AttributeValue {
        self.value.clone()
    }

    into_operand_builder!();
}

impl ValueBuilderImpl for ValueBuilder<Vec<Box<dyn ValueBuilderImpl>>> {
    fn attribute_value(&self) -> AttributeValue {
        if self.value.is_empty() {
            return AttributeValue::Null(true);
        }

        let value = self.value.iter().map(|x| x.attribute_value()).collect();

        AttributeValue::L(value)
    }

    into_operand_builder!();
}

impl ValueBuilderImpl for ValueBuilder<HashMap<String, Box<dyn ValueBuilderImpl>>> {
    fn attribute_value(&self) -> AttributeValue {
        if self.value.is_empty() {
            return AttributeValue::Null(true);
        }

        let value = self
            .value
            .iter()
            .map(|(k, v)| (k.clone(), v.attribute_value()))
            .collect();

        AttributeValue::M(value)
    }

    into_operand_builder!();
}

pub fn value<T>(value: T) -> Box<ValueBuilder<T>> {
    Box::new(ValueBuilder { value })
}

#[derive(Default, Debug, Clone)]
pub struct NameBuilder {
    name: String,
}

impl NameBuilder {
    pub fn size(self: Box<Self>) -> Box<SizeBuilder> {
        Box::new(SizeBuilder { name_builder: self })
    }

    pub fn if_not_exists(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder> {
        if_not_exists(self, right)
    }
}

impl OperandBuilder for NameBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        if self.name.is_empty() {
            bail!(ExpressionError::UnsetParameterError(
                "BuildOperand".to_owned(),
                "NameBuilder".to_owned(),
            ));
        }

        let mut node = ExpressionNode::default();

        let name_split = self.name.split('.');
        let mut fmt_names = Vec::new();

        for mut word in name_split {
            if word.is_empty() {
                bail!(ExpressionError::UnsetParameterError(
                    "BuildOperand".to_owned(),
                    "NameBuilder".to_owned(),
                ));
            }

            let mut substr = "";
            if word.chars().nth(word.len() - 1).unwrap() == ']' {
                for (j, ch) in word.chars().enumerate() {
                    if ch == '[' {
                        substr = &word[j..];
                        word = &word[..j];
                        break;
                    }
                }
            }

            if word.is_empty() {
                bail!(ExpressionError::UnsetParameterError(
                    "BuildOperand".to_owned(),
                    "NameBuilder".to_owned(),
                ));
            }

            // Create a string with special characters that can be substituted later: $p
            node.names.push(word.to_owned());
            fmt_names.push(format!("$n{}", substr));
        }

        node.fmt_expression = fmt_names.join(".");
        Ok(Operand::new(node))
    }
}

impl PlusBuilder for NameBuilder {}
impl MinusBuilder for NameBuilder {}
impl ListAppendBuilder for NameBuilder {}

pub fn name(name: impl Into<String>) -> Box<NameBuilder> {
    Box::new(NameBuilder { name: name.into() })
}

#[derive(Debug, Clone)]
pub struct SizeBuilder {
    name_builder: Box<NameBuilder>,
}

impl OperandBuilder for SizeBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        let mut operand = self.name_builder.build_operand()?;
        operand.expression_node.fmt_expression =
            format!("size ({})", operand.expression_node.fmt_expression);

        Ok(operand)
    }
}

pub fn size(name_builder: Box<NameBuilder>) -> Box<SizeBuilder> {
    name_builder.size()
}

#[derive(Debug, Clone)]
pub struct KeyBuilder {
    key: String,
}

impl OperandBuilder for KeyBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        if self.key.is_empty() {
            bail!(ExpressionError::UnsetParameterError(
                "BuildOperand".to_owned(),
                "KeyBuilder".to_owned(),
            ));
        }

        Ok(Operand::new(ExpressionNode::from_names(
            vec![self.key.clone()],
            "$n",
        )))
    }
}

pub fn key(key: impl Into<String>) -> Box<KeyBuilder> {
    Box::new(KeyBuilder { key: key.into() })
}

#[derive(Copy, Clone, PartialEq, Debug, Derivative)]
#[derivative(Default)]
enum SetValueMode {
    #[derivative(Default)]
    Unset,
    Plus,
    Minus,
    ListAppend,
    IfNotExists,
}

#[derive(Default)]
pub struct SetValueBuilder {
    left_operand: Option<Box<dyn OperandBuilder>>,
    right_operand: Option<Box<dyn OperandBuilder>>,
    mode: SetValueMode,
}

impl OperandBuilder for SetValueBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        if self.mode == SetValueMode::Unset {
            bail!(ExpressionError::UnsetParameterError(
                "BuildOperand".to_owned(),
                "SetValueBuilder".to_owned(),
            ));
        }

        let left = self.left_operand.as_ref().unwrap().build_operand()?;
        let left_node = left.expression_node;

        let right = self.right_operand.as_ref().unwrap().build_operand()?;
        let right_node = right.expression_node;

        let node = ExpressionNode::from_children_expression(
            vec![left_node, right_node],
            match self.mode {
                SetValueMode::Plus => "$c + $c",
                SetValueMode::Minus => "$c - $c",
                SetValueMode::ListAppend => "list_append($c, $c)",
                SetValueMode::IfNotExists => "if_not_exists($c, $c)",
                _ => bail!("build operand error: unsupported mode: {:?}", self.mode),
            }
            .to_owned(),
        );

        Ok(Operand::new(node))
    }
}

pub fn plus(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    Box::new(SetValueBuilder {
        left_operand: Some(left_operand),
        right_operand: Some(right_operand),
        mode: SetValueMode::Plus,
    })
}

pub fn minus(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    Box::new(SetValueBuilder {
        left_operand: Some(left_operand),
        right_operand: Some(right_operand),
        mode: SetValueMode::Minus,
    })
}

pub fn list_append(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    Box::new(SetValueBuilder {
        left_operand: Some(left_operand),
        right_operand: Some(right_operand),
        mode: SetValueMode::ListAppend,
    })
}

#[allow(clippy::boxed_local)]
pub fn if_not_exists(
    name: Box<NameBuilder>,
    value: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    Box::new(SetValueBuilder {
        left_operand: Some(name),
        right_operand: Some(value),
        mode: SetValueMode::IfNotExists,
    })
}

pub trait PlusBuilder: OperandBuilder {
    fn plus(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder>
    where
        Self: Sized + 'static,
    {
        plus(self, right)
    }
}

pub trait MinusBuilder: OperandBuilder {
    fn minus(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder>
    where
        Self: Sized + 'static,
    {
        minus(self, right)
    }
}

pub trait ListAppendBuilder: OperandBuilder {
    fn list_append(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder>
    where
        Self: Sized + 'static,
    {
        list_append(self, right)
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::model::AttributeValue;

    use crate::*;

    #[test]
    fn basic_name() -> anyhow::Result<()> {
        let input = name("foo");

        assert_eq!(
            input.build_operand()?.expression_node,
            ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
        );

        Ok(())
    }

    #[test]
    fn duplicate_name() -> anyhow::Result<()> {
        let input = name("foo.foo");

        assert_eq!(
            input.build_operand()?.expression_node,
            ExpressionNode::from_names(vec!["foo".to_owned(), "foo".to_owned()], "$n.$n"),
        );

        Ok(())
    }

    #[test]
    fn basic_value() -> anyhow::Result<()> {
        let input = value(5);

        assert_eq!(
            input.build_operand()?.expression_node,
            ExpressionNode::from_values(vec![AttributeValue::N("5".to_owned())], "$v"),
        );

        Ok(())
    }

    #[test]
    fn attribute_value_as_value() -> anyhow::Result<()> {
        let input = value(AttributeValue::N("5".to_owned()));

        assert_eq!(
            input.build_operand()?.expression_node,
            ExpressionNode::from_values(vec![AttributeValue::N("5".to_owned())], "$v"),
        );

        Ok(())
    }

    #[test]
    fn nested_name() -> anyhow::Result<()> {
        let input = name("foo.bar");

        assert_eq!(
            input.build_operand()?.expression_node,
            ExpressionNode::from_names(vec!["foo".to_owned(), "bar".to_owned()], "$n.$n"),
        );

        Ok(())
    }

    #[test]
    fn nested_name_with_index() -> anyhow::Result<()> {
        let input = name("foo.bar[0].baz");

        assert_eq!(
            input.build_operand()?.expression_node,
            ExpressionNode::from_names(
                vec!["foo".to_owned(), "bar".to_owned(), "baz".to_owned()],
                "$n.$n[0].$n"
            ),
        );

        Ok(())
    }

    #[test]
    fn basic_size() -> anyhow::Result<()> {
        let input = name("foo").size();

        assert_eq!(
            input.build_operand()?.expression_node,
            ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n)"),
        );

        Ok(())
    }

    #[test]
    fn basic_key() -> anyhow::Result<()> {
        let input = key("foo");

        assert_eq!(
            input.build_operand()?.expression_node,
            ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
        );

        Ok(())
    }

    #[test]
    fn unset_key_error() -> anyhow::Result<()> {
        let input = key("");

        assert_eq!(
            input
                .build_operand()
                .map_err(|e| e.downcast::<error::ExpressionError>().unwrap())
                .unwrap_err(),
            error::ExpressionError::UnsetParameterError(
                "BuildOperand".to_owned(),
                "KeyBuilder".to_owned()
            )
        );

        Ok(())
    }

    #[test]
    fn empty_name_error() -> anyhow::Result<()> {
        let input = name("");

        assert_eq!(
            input
                .build_operand()
                .map_err(|e| e.downcast::<error::ExpressionError>().unwrap())
                .unwrap_err(),
            error::ExpressionError::UnsetParameterError(
                "BuildOperand".to_owned(),
                "NameBuilder".to_owned()
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_name() -> anyhow::Result<()> {
        let input = name("foo..bar");

        assert_eq!(
            input
                .build_operand()
                .map_err(|e| e.downcast::<error::ExpressionError>().unwrap())
                .unwrap_err(),
            error::ExpressionError::UnsetParameterError(
                "BuildOperand".to_owned(),
                "NameBuilder".to_owned()
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_index() -> anyhow::Result<()> {
        let input = name("[foo]");

        assert_eq!(
            input
                .build_operand()
                .map_err(|e| e.downcast::<error::ExpressionError>().unwrap())
                .unwrap_err(),
            error::ExpressionError::UnsetParameterError(
                "BuildOperand".to_owned(),
                "NameBuilder".to_owned()
            )
        );

        Ok(())
    }
}
