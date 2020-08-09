use anyhow::bail;
use rusoto_dynamodb::AttributeValue;

use crate::ExpressionNode;

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

    fn into_boxed(self) -> Box<Self>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum ValueBuilder {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl OperandBuilder for ValueBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        let expr = match self {
            ValueBuilder::Bool(b) => AttributeValue {
                bool: Some(*b),
                ..Default::default()
            },
            ValueBuilder::Int(n) => AttributeValue {
                n: Some(n.to_string()),
                ..Default::default()
            },
            ValueBuilder::Float(n) => AttributeValue {
                n: Some(n.to_string()),
                ..Default::default()
            },
            ValueBuilder::String(s) => AttributeValue {
                s: Some(s.clone()),
                ..Default::default()
            },
        };

        let node = ExpressionNode::from_values(vec![expr], "$v");
        Ok(Operand::new(node))
    }
}

impl PlusBuilder for ValueBuilder {}
impl MinusBuilder for ValueBuilder {}
impl ListAppendBuilder for ValueBuilder {}

pub fn bool_value(value: bool) -> Box<ValueBuilder> {
    ValueBuilder::Bool(value).into_boxed()
}

pub fn int_value(value: i64) -> Box<ValueBuilder> {
    ValueBuilder::Int(value).into_boxed()
}

pub fn float_value(value: f64) -> Box<ValueBuilder> {
    ValueBuilder::Float(value).into_boxed()
}

pub fn string_value(value: impl Into<String>) -> Box<ValueBuilder> {
    ValueBuilder::String(value.into()).into_boxed()
}

pub struct NameBuilder {
    name: String,
}

impl NameBuilder {
    pub fn size(self: Box<Self>) -> Box<SizeBuilder> {
        SizeBuilder { name_builder: self }.into_boxed()
    }

    pub fn if_not_exists(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder> {
        if_not_exists(self, right)
    }
}

impl OperandBuilder for NameBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        if self.name.is_empty() {
            bail!("NameBuilder BuildOperand");
        }

        let mut node = ExpressionNode::default();

        let name_split = self.name.split('.');
        let mut fmt_names = Vec::new();

        for mut word in name_split {
            if word.is_empty() {
                bail!("NameBuilder BuildOperand");
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
                bail!("NameBuilder BuildOperand");
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
    NameBuilder { name: name.into() }.into_boxed()
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
            bail!("KeyBuilder build_operand unset");
        }

        Ok(Operand::new(ExpressionNode::from_names(
            vec![self.key.clone()],
            "$n",
        )))
    }
}

pub fn key(key: impl Into<String>) -> Box<KeyBuilder> {
    KeyBuilder { key: key.into() }.into_boxed()
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
        let left = self.left_operand.build_operand()?;
        let left_node = left.expression_node;

        let right = self.right_operand.build_operand()?;
        let right_node = right.expression_node;

        let mut node = ExpressionNode::from_children(vec![left_node, right_node]);

        node.fmt_expression = match self.mode {
            SetValueMode::Plus => "$c + $c",
            SetValueMode::Minus => "$c - $c",
            SetValueMode::ListAppend => "list_append($c, $c)",
            SetValueMode::IfNotExists => "if_not_exists($c, $c)",
        }
        .to_owned();

        Ok(Operand::new(node))
    }
}

pub fn plus(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::Plus,
    }
    .into_boxed()
}

pub fn minus(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::Minus,
    }
    .into_boxed()
}

pub fn list_append(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::ListAppend,
    }
    .into_boxed()
}

#[allow(clippy::boxed_local)]
pub fn if_not_exists(
    name: Box<NameBuilder>,
    value: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    SetValueBuilder {
        left_operand: name.into_boxed(),
        right_operand: value,
        mode: SetValueMode::IfNotExists,
    }
    .into_boxed()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name_builder() {
        let builder = name("test");
    }

    #[test]
    fn test_value_builder() {
        let builder = ValueBuilder::String("test".to_owned());
    }

    #[test]
    fn test_key_builder() {
        let builder = key("test");
    }

    #[test]
    fn test_size_builder() {
        let builder = size(name("test"));

        let builder = name("test").size();
    }

    #[test]
    fn test_size_builder_plus() {
        // TODO: set()

        let expr = plus(int_value(10), int_value(5));

        let expr = name("test").plus(int_value(10));

        let expr = int_value(10).plus(int_value(5));
    }

    #[test]
    fn test_size_builder_minus() {
        // TODO: set()

        let expr = minus(int_value(10), int_value(5));

        let expr = name("test").minus(int_value(10));

        let expr = int_value(10).minus(int_value(5));
    }

    #[test]
    fn test_size_builder_list_append() {
        // TODO: set()

        let expr = list_append(int_value(10), int_value(5));

        let expr = name("test").list_append(int_value(10));

        let expr = int_value(10).list_append(int_value(5));
    }

    #[test]
    fn test_size_builder_if_not_exists() {
        // TODO: set()

        let expr = if_not_exists(name("test"), int_value(0));

        let expr = name("test").if_not_exists(int_value(10));
    }
}
