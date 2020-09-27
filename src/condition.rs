use anyhow::bail;
use derivative::*;

use crate::{
    error::ExpressionError, value, ExpressionNode, NameBuilder, OperandBuilder, SizeBuilder,
    TreeBuilder,
};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/condition.go

#[derive(Copy, Clone, PartialEq, Debug, Derivative)]
#[derivative(Default)]
enum ConditionMode {
    #[derivative(Default)]
    Unset,
    Equal,
    NotEqual,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
    And,
    Or,
    Not,
    Between,
    In,
    AttrExists,
    AttrNotExists,
    AttrType,
    BeginsWith,
    Contains,
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum DynamoDBAttributeType {
    String,
    StringSet,
    Number,
    NumberSet,
    Binary,
    BinarySet,
    Boolean,
    Null,
    List,
    Map,
}

impl DynamoDBAttributeType {
    pub fn as_str(&self) -> &str {
        match self {
            DynamoDBAttributeType::String => "S",
            DynamoDBAttributeType::StringSet => "SS",
            DynamoDBAttributeType::Number => "N",
            DynamoDBAttributeType::NumberSet => "NS",
            DynamoDBAttributeType::Binary => "B",
            DynamoDBAttributeType::BinarySet => "BS",
            DynamoDBAttributeType::Boolean => "BOOL",
            DynamoDBAttributeType::Null => "NULL",
            DynamoDBAttributeType::List => "L",
            DynamoDBAttributeType::Map => "M",
        }
    }
}

#[derive(Default)]
pub struct ConditionBuilder {
    operand_list: Vec<Box<dyn OperandBuilder>>,
    condition_list: Vec<ConditionBuilder>,
    mode: ConditionMode,
}

impl ConditionBuilder {
    // TODO: variadic
    pub fn and(self, right: ConditionBuilder) -> ConditionBuilder {
        and(self, right)
    }

    // TODO: variadic
    pub fn or(self, right: ConditionBuilder) -> ConditionBuilder {
        or(self, right)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> ConditionBuilder {
        not(self)
    }

    fn build_child_nodes(&self) -> anyhow::Result<Vec<ExpressionNode>> {
        let mut child_nodes = Vec::new();

        for condition in self.condition_list.iter() {
            let node = condition.build_tree()?;
            child_nodes.push(node);
        }

        for ope in self.operand_list.iter() {
            let operand = ope.build_operand()?;
            child_nodes.push(operand.expression_node);
        }

        Ok(child_nodes)
    }

    fn compare_build_condition(
        mode: ConditionMode,
        mut node: ExpressionNode,
    ) -> anyhow::Result<ExpressionNode> {
        match mode {
            ConditionMode::Equal => node.fmt_expression = "$c = $c".to_owned(),
            ConditionMode::NotEqual => node.fmt_expression = "$c <> $c".to_owned(),
            ConditionMode::LessThan => node.fmt_expression = "$c < $c".to_owned(),
            ConditionMode::LessThanEqual => node.fmt_expression = "$c <= $c".to_owned(),
            ConditionMode::GreaterThan => node.fmt_expression = "$c > $c".to_owned(),
            ConditionMode::GreaterThanEqual => node.fmt_expression = "$c >= $c".to_owned(),
            _ => bail!(
                "build compare condition error: unsupported mode: {:?}",
                mode
            ),
        }
        Ok(node)
    }

    fn compound_build_condition(
        condition_builder: &ConditionBuilder,
        mut node: ExpressionNode,
    ) -> anyhow::Result<ExpressionNode> {
        // create a string with escaped characters to substitute them with proper
        // aliases during runtime
        let mode = match condition_builder.mode {
            ConditionMode::And => " AND ",
            ConditionMode::Or => " OR ",
            _ => bail!(
                "build compound condition error: unsupported mode: {:?}",
                condition_builder.mode
            ),
        };

        node.fmt_expression = format!(
            "($c){}",
            format!("{}{}", mode, "($c)").repeat(condition_builder.condition_list.len() - 1)
        );

        Ok(node)
    }

    fn not_build_condition(mut node: ExpressionNode) -> anyhow::Result<ExpressionNode> {
        // create a string with escaped characters to substitute them with proper
        // aliases during runtime
        node.fmt_expression = "NOT ($c)".to_owned();

        Ok(node)
    }

    fn between_build_condition(mut node: ExpressionNode) -> anyhow::Result<ExpressionNode> {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "$c BETWEEN $c AND $c".to_owned();

        Ok(node)
    }

    fn in_build_condition(
        condition_builder: &ConditionBuilder,
        mut node: ExpressionNode,
    ) -> anyhow::Result<ExpressionNode> {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = format!(
            "$c IN ($c{})",
            ", $c".repeat(condition_builder.operand_list.len() - 2)
        );

        Ok(node)
    }

    fn attr_exists_build_condition(mut node: ExpressionNode) -> anyhow::Result<ExpressionNode> {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "attribute_exists ($c)".to_owned();

        Ok(node)
    }

    fn attr_not_exists_build_condition(mut node: ExpressionNode) -> anyhow::Result<ExpressionNode> {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "attribute_not_exists ($c)".to_owned();

        Ok(node)
    }

    fn attr_type_build_condition(mut node: ExpressionNode) -> anyhow::Result<ExpressionNode> {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "attribute_type ($c, $c)".to_owned();

        Ok(node)
    }

    fn begins_with_build_condition(mut node: ExpressionNode) -> anyhow::Result<ExpressionNode> {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "begins_with ($c, $c)".to_owned();

        Ok(node)
    }

    fn contains_build_condition(mut node: ExpressionNode) -> anyhow::Result<ExpressionNode> {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "contains ($c, $c)".to_owned();

        Ok(node)
    }
}

impl TreeBuilder for ConditionBuilder {
    fn build_tree(&self) -> anyhow::Result<ExpressionNode> {
        let child_nodes = self.build_child_nodes()?;
        let ret = ExpressionNode::from_children(child_nodes);

        match self.mode {
            ConditionMode::Equal
            | ConditionMode::NotEqual
            | ConditionMode::LessThan
            | ConditionMode::LessThanEqual
            | ConditionMode::GreaterThan
            | ConditionMode::GreaterThanEqual => {
                Ok(ConditionBuilder::compare_build_condition(self.mode, ret)?)
            }
            ConditionMode::And | ConditionMode::Or => {
                Ok(ConditionBuilder::compound_build_condition(self, ret)?)
            }
            ConditionMode::Not => Ok(ConditionBuilder::not_build_condition(ret)?),
            ConditionMode::Between => Ok(ConditionBuilder::between_build_condition(ret)?),
            ConditionMode::In => Ok(ConditionBuilder::in_build_condition(self, ret)?),
            ConditionMode::AttrExists => Ok(ConditionBuilder::attr_exists_build_condition(ret)?),
            ConditionMode::AttrNotExists => {
                Ok(ConditionBuilder::attr_not_exists_build_condition(ret)?)
            }
            ConditionMode::AttrType => Ok(ConditionBuilder::attr_type_build_condition(ret)?),
            ConditionMode::BeginsWith => Ok(ConditionBuilder::begins_with_build_condition(ret)?),
            ConditionMode::Contains => Ok(ConditionBuilder::contains_build_condition(ret)?),
            ConditionMode::Unset => bail!(ExpressionError::UnsetParameterError(
                "buildTree".to_owned(),
                "ConditionBuilder".to_owned(),
            )),
            //_ => bail!("build condition error: unsupported mode: {:?}", self.mode),
        }
    }
}

pub fn equal(left: Box<dyn OperandBuilder>, right: Box<dyn OperandBuilder>) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::Equal,
    }
}

pub fn not_equal(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::NotEqual,
    }
}

pub fn less_than(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::LessThan,
    }
}

pub fn less_than_equal(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::LessThanEqual,
    }
}

pub fn greater_than(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::GreaterThan,
    }
}

pub fn greater_than_equal(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::GreaterThanEqual,
    }
}

// TODO: variadic
pub fn and(left: ConditionBuilder, right: ConditionBuilder) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Vec::new(),
        condition_list: vec![left, right],
        mode: ConditionMode::And,
    }
}

// TODO: variadic
pub fn or(left: ConditionBuilder, right: ConditionBuilder) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Vec::new(),
        condition_list: vec![left, right],
        mode: ConditionMode::Or,
    }
}

pub fn not(condition_builder: ConditionBuilder) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Vec::new(),
        condition_list: vec![condition_builder],
        mode: ConditionMode::Not,
    }
}

pub fn between(
    op: Box<dyn OperandBuilder>,
    lower: Box<dyn OperandBuilder>,
    upper: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![op, lower, upper],
        condition_list: Vec::new(),
        mode: ConditionMode::Between,
    }
}

pub fn r#in(
    left: Box<dyn OperandBuilder>,
    mut right: Vec<Box<dyn OperandBuilder>>,
) -> ConditionBuilder {
    let mut operand_list = vec![left];
    operand_list.append(&mut right);

    ConditionBuilder {
        operand_list,
        condition_list: Vec::new(),
        mode: ConditionMode::In,
    }
}

pub fn attribute_exists(name: Box<NameBuilder>) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![name],
        condition_list: Vec::new(),
        mode: ConditionMode::AttrExists,
    }
}

pub fn attribute_not_exists(name: Box<NameBuilder>) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![name],
        condition_list: Vec::new(),
        mode: ConditionMode::AttrNotExists,
    }
}

pub fn attribute_type(
    name: Box<NameBuilder>,
    attr_type: DynamoDBAttributeType,
) -> ConditionBuilder {
    let v = value(attr_type.as_str().to_owned());
    ConditionBuilder {
        operand_list: vec![name, v],
        condition_list: Vec::new(),
        mode: ConditionMode::AttrType,
    }
}

pub fn begins_with(name: Box<NameBuilder>, prefix: impl Into<String>) -> ConditionBuilder {
    let v = value(prefix.into());
    ConditionBuilder {
        operand_list: vec![name, v],
        condition_list: Vec::new(),
        mode: ConditionMode::BeginsWith,
    }
}

pub fn contains(name: Box<NameBuilder>, substr: impl Into<String>) -> ConditionBuilder {
    let v = value(substr.into());
    ConditionBuilder {
        operand_list: vec![name, v],
        condition_list: Vec::new(),
        mode: ConditionMode::Contains,
    }
}

pub trait EqualBuilder: OperandBuilder {
    fn equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        equal(self, right)
    }
}

pub trait NotEqualBuilder: OperandBuilder {
    fn not_equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        not_equal(self, right)
    }
}

pub trait LessThanBuilder: OperandBuilder {
    fn less_than(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        less_than(self, right)
    }
}

pub trait LessThanEqualBuilder: OperandBuilder {
    fn less_than_equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        less_than_equal(self, right)
    }
}

pub trait GreaterThanBuilder: OperandBuilder {
    fn greater_than(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        greater_than(self, right)
    }
}

pub trait GreaterThanEqualBuilder: OperandBuilder {
    fn greater_than_equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        greater_than_equal(self, right)
    }
}

pub trait BetweenBuilder: OperandBuilder {
    fn between(
        self: Box<Self>,
        upper: Box<dyn OperandBuilder>,
        lower: Box<dyn OperandBuilder>,
    ) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        between(self, upper, lower)
    }
}

pub trait InBuilder: OperandBuilder {
    fn r#in(self: Box<Self>, right: Vec<Box<dyn OperandBuilder>>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        r#in(self, right)
    }
}

impl NameBuilder {
    pub fn attribute_exists(self: Box<NameBuilder>) -> ConditionBuilder {
        attribute_exists(self)
    }

    pub fn attribute_not_exists(self: Box<NameBuilder>) -> ConditionBuilder {
        attribute_not_exists(self)
    }

    pub fn attribute_type(
        self: Box<NameBuilder>,
        attr_type: DynamoDBAttributeType,
    ) -> ConditionBuilder {
        attribute_type(self, attr_type)
    }

    pub fn begins_with(self: Box<NameBuilder>, prefix: impl Into<String>) -> ConditionBuilder {
        begins_with(self, prefix)
    }

    pub fn contains(self: Box<NameBuilder>, substr: impl Into<String>) -> ConditionBuilder {
        contains(self, substr)
    }
}

impl EqualBuilder for NameBuilder {}
impl NotEqualBuilder for NameBuilder {}
impl LessThanBuilder for NameBuilder {}
impl LessThanEqualBuilder for NameBuilder {}
impl GreaterThanBuilder for NameBuilder {}
impl GreaterThanEqualBuilder for NameBuilder {}
impl BetweenBuilder for NameBuilder {}
impl InBuilder for NameBuilder {}

impl EqualBuilder for SizeBuilder {}
impl NotEqualBuilder for SizeBuilder {}
impl LessThanBuilder for SizeBuilder {}
impl LessThanEqualBuilder for SizeBuilder {}
impl GreaterThanBuilder for SizeBuilder {}
impl GreaterThanEqualBuilder for SizeBuilder {}
impl BetweenBuilder for SizeBuilder {}
impl InBuilder for SizeBuilder {}

#[cfg(test)]
mod tests {
    use rusoto_dynamodb::AttributeValue;

    use crate::*;

    #[test]
    fn name_equal_name() -> anyhow::Result<()> {
        let input = name("foo").equal(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n")
                ],
                "$c = $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_equal_value() -> anyhow::Result<()> {
        let input = value(5).equal(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                ],
                "$c = $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_equal_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().equal(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c = $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_not_equal_name() -> anyhow::Result<()> {
        let input = name("foo").not_equal(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c <> $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_not_equal_value() -> anyhow::Result<()> {
        let input = value(5).not_equal(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c <> $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_not_equal_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().not_equal(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c <> $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_less_than_name() -> anyhow::Result<()> {
        let input = name("foo").less_than(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c < $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_less_than_value() -> anyhow::Result<()> {
        let input = value(5).less_than(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c < $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_less_than_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().less_than(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c < $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_less_than_equal_name() -> anyhow::Result<()> {
        let input = name("foo").less_than_equal(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c <= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_less_than_equal_value() -> anyhow::Result<()> {
        let input = value(5).less_than_equal(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c <= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_less_than_equal_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().less_than_equal(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c <= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_greater_than_name() -> anyhow::Result<()> {
        let input = name("foo").greater_than(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c > $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_greater_than_value() -> anyhow::Result<()> {
        let input = value(5).greater_than(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c > $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_greater_than_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().greater_than(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c > $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_greater_than_equal_name() -> anyhow::Result<()> {
        let input = name("foo").greater_than_equal(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c >= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_greater_than_equal_value() -> anyhow::Result<()> {
        let input = value(5).greater_than_equal(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c >= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_greater_than_equal_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().greater_than_equal(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c >= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_operand_error_equal() -> anyhow::Result<()> {
        let input = name("").size().equal(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_not_equal() -> anyhow::Result<()> {
        let input = name("").size().not_equal(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_less_than() -> anyhow::Result<()> {
        let input = name("").size().less_than(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_less_than_equal() -> anyhow::Result<()> {
        let input = name("").size().less_than_equal(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_greater_than() -> anyhow::Result<()> {
        let input = name("").size().greater_than(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_greater_than_equal() -> anyhow::Result<()> {
        let input = name("").size().greater_than_equal(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn no_match_error() -> anyhow::Result<()> {
        let input = ConditionBuilder::default();

        assert_eq!(
            input
                .build_tree()
                .map_err(|e| e.downcast::<error::ExpressionError>().unwrap())
                .unwrap_err(),
            error::ExpressionError::UnsetParameterError(
                "buildTree".to_owned(),
                "ConditionBuilder".to_owned()
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_and() -> anyhow::Result<()> {
        let input = name("foo")
            .equal(value(5))
            .and(name("bar").equal(value("baz")));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_children_expression(
                        vec![
                            ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                            ExpressionNode::from_values(
                                vec![AttributeValue {
                                    n: Some("5".to_owned()),
                                    ..Default::default()
                                }],
                                "$v"
                            )
                        ],
                        "$c = $c"
                    ),
                    ExpressionNode::from_children_expression(
                        vec![
                            ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                            ExpressionNode::from_values(
                                vec![AttributeValue {
                                    s: Some("baz".to_owned()),
                                    ..Default::default()
                                }],
                                "$v"
                            )
                        ],
                        "$c = $c"
                    ),
                ],
                "($c) AND ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_or() -> anyhow::Result<()> {
        let input = name("foo")
            .equal(value(5))
            .or(name("bar").equal(value("baz")));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_children_expression(
                        vec![
                            ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                            ExpressionNode::from_values(
                                vec![AttributeValue {
                                    n: Some("5".to_owned()),
                                    ..Default::default()
                                }],
                                "$v"
                            )
                        ],
                        "$c = $c"
                    ),
                    ExpressionNode::from_children_expression(
                        vec![
                            ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                            ExpressionNode::from_values(
                                vec![AttributeValue {
                                    s: Some("baz".to_owned()),
                                    ..Default::default()
                                }],
                                "$v"
                            )
                        ],
                        "$c = $c"
                    ),
                ],
                "($c) OR ($c)"
            )
        );

        Ok(())
    }

    // TODO: variadic tests require a macro with variadic arguments

    #[test]
    fn invalid_operand_error_and() -> anyhow::Result<()> {
        let input = name("")
            .size()
            .greater_than_equal(value(5))
            .and(name("[5]").between(value(3), value(9)));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_or() -> anyhow::Result<()> {
        let input = name("")
            .size()
            .greater_than_equal(value(5))
            .or(name("[5]").between(value(3), value(9)));

        assert_eq!(
            input
                .build_tree()
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
    fn basic_method_not() -> anyhow::Result<()> {
        let input = name("foo").equal(value(5)).not();

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![ExpressionNode::from_children_expression(
                    vec![
                        ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                        ExpressionNode::from_values(
                            vec![AttributeValue {
                                n: Some("5".to_owned()),
                                ..Default::default()
                            }],
                            "$v"
                        )
                    ],
                    "$c = $c"
                )],
                "NOT ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_function_not() -> anyhow::Result<()> {
        let input = not(name("foo").equal(value(5)));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![ExpressionNode::from_children_expression(
                    vec![
                        ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                        ExpressionNode::from_values(
                            vec![AttributeValue {
                                n: Some("5".to_owned()),
                                ..Default::default()
                            }],
                            "$v"
                        )
                    ],
                    "$c = $c"
                )],
                "NOT ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_operand_error_not() -> anyhow::Result<()> {
        let input = name("")
            .size()
            .greater_than_equal(value(5))
            .or(name("[5]").between(value(3), value(9)))
            .not();

        assert_eq!(
            input
                .build_tree()
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
    fn basic_method_between_for_name() -> anyhow::Result<()> {
        let input = name("foo").between(value(5), value(7));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c BETWEEN $c AND $c"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_between_for_value() -> anyhow::Result<()> {
        let input = value(6).between(value(5), value(7));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("6".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c BETWEEN $c AND $c"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_between_for_size() -> anyhow::Result<()> {
        let input = name("foo").size().between(value(5), value(7));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n)"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c BETWEEN $c AND $c"
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_operand_error_between() -> anyhow::Result<()> {
        let input = name("[5]").between(value(3), name("foo..bar"));

        assert_eq!(
            input
                .build_tree()
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
    fn basic_method_in_for_name() -> anyhow::Result<()> {
        let input = name("foo").r#in(vec![value(5), value(7)]);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c IN ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_in_for_value() -> anyhow::Result<()> {
        let input = value(6).r#in(vec![value(5), value(7)]);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("6".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c IN ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_in_for_size() -> anyhow::Result<()> {
        let input = name("foo").size().r#in(vec![value(5), value(7)]);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n)"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c IN ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_operand_error_in() -> anyhow::Result<()> {
        let input = name("[5]").r#in(vec![value(3), name("foo..bar")]);

        assert_eq!(
            input
                .build_tree()
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
    fn basic_attr_exists() -> anyhow::Result<()> {
        let input = name("foo").attribute_exists();

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![ExpressionNode::from_names(vec!["foo".to_owned()], "$n")],
                "attribute_exists ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_attr_not_exists() -> anyhow::Result<()> {
        let input = name("foo").attribute_not_exists();

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![ExpressionNode::from_names(vec!["foo".to_owned()], "$n")],
                "attribute_not_exists ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_operand_error_attr_exists() -> anyhow::Result<()> {
        let input = attribute_exists(name(""));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_not_attr_exists() -> anyhow::Result<()> {
        let input = attribute_not_exists(name(""));

        assert_eq!(
            input
                .build_tree()
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
    fn attr_type_string() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDBAttributeType::String);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("S".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_stringset() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDBAttributeType::StringSet);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("SS".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_number() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDBAttributeType::Number);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("N".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_binaryset() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDBAttributeType::BinarySet);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("BS".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_boolean() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDBAttributeType::Boolean);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("BOOL".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_null() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDBAttributeType::Null);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("NULL".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_list() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDBAttributeType::List);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("L".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_map() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDBAttributeType::Map);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("M".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_invalid_operand() -> anyhow::Result<()> {
        let input = name("").attribute_type(DynamoDBAttributeType::Map);

        assert_eq!(
            input
                .build_tree()
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
    fn basic_begins_with() -> anyhow::Result<()> {
        let input = name("foo").begins_with("bar");

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "begins_with ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn begins_with_invalid_operand() -> anyhow::Result<()> {
        let input = name("").begins_with("bar");

        assert_eq!(
            input
                .build_tree()
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
    fn basic_contains() -> anyhow::Result<()> {
        let input = name("foo").contains("bar");

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "contains ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn contains_invalid_operand() -> anyhow::Result<()> {
        let input = name("").contains("bar");

        assert_eq!(
            input
                .build_tree()
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
    fn compound_and() -> anyhow::Result<()> {
        let input = ConditionBuilder {
            condition_list: vec![
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
            ],
            mode: condition::ConditionMode::And,
            ..Default::default()
        };

        assert_eq!(
            ConditionBuilder::compound_build_condition(&input, ExpressionNode::default())?
                .fmt_expression,
            "($c) AND ($c) AND ($c) AND ($c)",
        );

        Ok(())
    }

    #[test]
    fn compound_or() -> anyhow::Result<()> {
        let input = ConditionBuilder {
            condition_list: vec![
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
            ],
            mode: condition::ConditionMode::Or,
            ..Default::default()
        };

        assert_eq!(
            ConditionBuilder::compound_build_condition(&input, ExpressionNode::default())?
                .fmt_expression,
            "($c) OR ($c) OR ($c) OR ($c) OR ($c) OR ($c) OR ($c)",
        );

        Ok(())
    }

    #[test]
    fn in_and() -> anyhow::Result<()> {
        let input = ConditionBuilder {
            operand_list: vec![
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
            ],
            mode: condition::ConditionMode::And,
            ..Default::default()
        };

        assert_eq!(
            ConditionBuilder::in_build_condition(&input, ExpressionNode::default())?.fmt_expression,
            "$c IN ($c, $c, $c, $c, $c, $c)",
        );

        Ok(())
    }
}
