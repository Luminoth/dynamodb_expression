use anyhow::bail;
use rusoto_dynamodb::AttributeValue;

use crate::{error::ExpressionError, ExpressionNode};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/operand.go

pub struct Operand {
    pub(crate) expression_node: ExpressionNode,
}

impl Operand {
    fn new(expression_node: ExpressionNode) -> Self {
        Self { expression_node }
    }
}

pub trait OperandBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand>;
}

// marker trait for working with generic ValueBuilders
pub trait ValueBuilderImpl: OperandBuilder {
    fn into_operand_builder(self: Box<Self>) -> Box<dyn OperandBuilder>;
}

pub struct ValueBuilder<T> {
    value: T,
}

impl<T> ValueBuilder<T> {}

macro_rules! impl_value_builder {
    ($type:ty) => {
        impl ValueBuilderImpl for ValueBuilder<$type> {
            fn into_operand_builder(self: Box<Self>) -> Box<dyn OperandBuilder> {
                self
            }
        }

        impl PlusBuilder for ValueBuilder<$type> {}
        impl MinusBuilder for ValueBuilder<$type> {}
        impl ListAppendBuilder for ValueBuilder<$type> {}
    };
}

impl_value_builder!(bool);

impl OperandBuilder for ValueBuilder<bool> {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        let expr = AttributeValue {
            bool: Some(self.value),
            ..Default::default()
        };

        let node = ExpressionNode::from_values(vec![expr], "$v");
        Ok(Operand::new(node))
    }
}

impl_value_builder!(i64);

impl OperandBuilder for ValueBuilder<i64> {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        let expr = AttributeValue {
            n: Some(self.value.to_string()),
            ..Default::default()
        };

        let node = ExpressionNode::from_values(vec![expr], "$v");
        Ok(Operand::new(node))
    }
}

impl_value_builder!(f64);

impl OperandBuilder for ValueBuilder<f64> {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        let expr = AttributeValue {
            n: Some(self.value.to_string()),
            ..Default::default()
        };

        let node = ExpressionNode::from_values(vec![expr], "$v");
        Ok(Operand::new(node))
    }
}

impl_value_builder!(&'static str);

impl OperandBuilder for ValueBuilder<&str> {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        let expr = AttributeValue {
            s: Some(self.value.to_owned()),
            ..Default::default()
        };

        let node = ExpressionNode::from_values(vec![expr], "$v");
        Ok(Operand::new(node))
    }
}

impl_value_builder!(String);

impl OperandBuilder for ValueBuilder<String> {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        let expr = AttributeValue {
            s: Some(self.value.clone()),
            ..Default::default()
        };

        let node = ExpressionNode::from_values(vec![expr], "$v");
        Ok(Operand::new(node))
    }
}

pub fn value<T>(value: T) -> Box<ValueBuilder<T>> {
    Box::new(ValueBuilder { value })
}

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

#[derive(Copy, Clone, PartialEq, Debug)]
enum SetValueMode {
    //Unset,
    Plus,
    Minus,
    ListAppend,
    IfNotExists,
}

pub struct SetValueBuilder {
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
    mode: SetValueMode,
}

impl OperandBuilder for SetValueBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        /*if self.mode == SetValueMode::Unset {
            bail!(ExpressionError::UnsetParameterError(
                "BuildOperand".to_owned(),
                "SetValueBuilder".to_owned(),
            ));
        }*/

        let left = self.left_operand.build_operand()?;
        let left_node = left.expression_node;

        let right = self.right_operand.build_operand()?;
        let right_node = right.expression_node;

        let node = ExpressionNode::from_children_expression(
            vec![left_node, right_node],
            match self.mode {
                SetValueMode::Plus => "$c + $c",
                SetValueMode::Minus => "$c - $c",
                SetValueMode::ListAppend => "list_append($c, $c)",
                SetValueMode::IfNotExists => "if_not_exists($c, $c)",
                //_ => bail!("build operand error: unsupported mode: {:?}", self.mode),
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
        left_operand,
        right_operand,
        mode: SetValueMode::Plus,
    })
}

pub fn minus(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    Box::new(SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::Minus,
    })
}

pub fn list_append(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    Box::new(SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::ListAppend,
    })
}

#[allow(clippy::boxed_local)]
pub fn if_not_exists(
    name: Box<NameBuilder>,
    value: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    Box::new(SetValueBuilder {
        left_operand: name,
        right_operand: value,
        mode: SetValueMode::IfNotExists,
    })
}

trait PlusBuilder: OperandBuilder {
    fn plus(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder>
    where
        Self: Sized + 'static,
    {
        plus(self, right)
    }
}

trait MinusBuilder: OperandBuilder {
    fn minus(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder>
    where
        Self: Sized + 'static,
    {
        minus(self, right)
    }
}

trait ListAppendBuilder: OperandBuilder {
    fn list_append(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder>
    where
        Self: Sized + 'static,
    {
        list_append(self, right)
    }
}
