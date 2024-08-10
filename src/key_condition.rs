//! Ported from [key_condition.go](https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/key_condition.go)

use anyhow::bail;
use derivative::*;

use crate::{
    error::ExpressionError, value, ExpressionNode, KeyBuilder, OperandBuilder, TreeBuilder,
    ValueBuilderImpl,
};

#[derive(Copy, Clone, PartialEq, Debug, Derivative)]
#[derivative(Default)]
enum KeyConditionMode {
    #[derivative(Default)]
    Unset,
    Invalid,
    Equal,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
    And,
    Between,
    BeginsWith,
}

#[derive(Default)]
pub struct KeyConditionBuilder {
    operand_list: Vec<Box<dyn OperandBuilder>>,
    key_condition_list: Vec<KeyConditionBuilder>,
    mode: KeyConditionMode,
}

impl KeyConditionBuilder {
    pub fn and(self, right: KeyConditionBuilder) -> KeyConditionBuilder {
        key_and(self, right)
    }

    fn build_child_nodes(&self) -> anyhow::Result<Vec<ExpressionNode>> {
        let mut child_nodes = Vec::new();

        for key_condition in self.key_condition_list.iter() {
            let node = key_condition.build_tree()?;
            child_nodes.push(node);
        }

        for ope in self.operand_list.iter() {
            let operand = ope.build_operand()?;
            child_nodes.push(operand.expression_node);
        }

        Ok(child_nodes)
    }

    fn compare_build_key_condition(
        mode: KeyConditionMode,
        mut node: ExpressionNode,
    ) -> anyhow::Result<ExpressionNode> {
        match mode {
            KeyConditionMode::Equal => "$c = $c".clone_into(&mut node.fmt_expression),
            KeyConditionMode::LessThan => "$c < $c".clone_into(&mut node.fmt_expression),
            KeyConditionMode::LessThanEqual => "$c <= $c".clone_into(&mut node.fmt_expression),
            KeyConditionMode::GreaterThan => "$c > $c".clone_into(&mut node.fmt_expression),
            KeyConditionMode::GreaterThanEqual => "$c >= $c".clone_into(&mut node.fmt_expression),
            _ => bail!(
                "build compare key condition error: unsupported mode: {:?}",
                mode
            ),
        }
        Ok(node)
    }

    fn and_build_key_condition(
        key_condition_builder: &KeyConditionBuilder,
        mut node: ExpressionNode,
    ) -> anyhow::Result<ExpressionNode> {
        if key_condition_builder.key_condition_list.is_empty()
            && key_condition_builder.operand_list.is_empty()
        {
            bail!(ExpressionError::InvalidParameterError(
                "andBuildKeyCondition".to_owned(),
                "KeyConditionBuilder".to_owned(),
            ));
        }

        // create a string with escaped characters to substitute them with proper
        // aliases during runtime
        "($c) AND ($c)".clone_into(&mut node.fmt_expression);

        Ok(node)
    }

    fn between_build_condition(mut node: ExpressionNode) -> ExpressionNode {
        // Create a string with special characters that can be substituted later: $c
        "$c BETWEEN $c AND $c".clone_into(&mut node.fmt_expression);

        node
    }

    fn begins_with_build_condition(mut node: ExpressionNode) -> ExpressionNode {
        // Create a string with special characters that can be substituted later: $c
        "begins_with ($c, $c)".clone_into(&mut node.fmt_expression);

        node
    }
}

impl TreeBuilder for KeyConditionBuilder {
    fn build_tree(&self) -> anyhow::Result<ExpressionNode> {
        let child_nodes = self.build_child_nodes()?;
        let ret = ExpressionNode::from_children(child_nodes);

        match self.mode {
            KeyConditionMode::Equal
            | KeyConditionMode::LessThan
            | KeyConditionMode::LessThanEqual
            | KeyConditionMode::GreaterThan
            | KeyConditionMode::GreaterThanEqual => Ok(
                KeyConditionBuilder::compare_build_key_condition(self.mode, ret)?,
            ),
            KeyConditionMode::And => Ok(KeyConditionBuilder::and_build_key_condition(self, ret)?),
            KeyConditionMode::Between => Ok(KeyConditionBuilder::between_build_condition(ret)),
            KeyConditionMode::BeginsWith => {
                Ok(KeyConditionBuilder::begins_with_build_condition(ret))
            }
            KeyConditionMode::Unset => bail!(ExpressionError::UnsetParameterError(
                "buildTree".to_owned(),
                "KeyConditionBuilder".to_owned(),
            )),
            KeyConditionMode::Invalid => {
                bail!("buildKeyCondition error: invalid key condition constructed")
            } //_ => bail!("buildKeyCondition error: unsupported mode: {:?}", self.mode),
        }
    }
}

pub fn key_equal(key: Box<KeyBuilder>, value: Box<dyn ValueBuilderImpl>) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: vec![key, value.into_operand_builder()],
        key_condition_list: Vec::new(),
        mode: KeyConditionMode::Equal,
    }
}

pub fn key_less_than(
    key: Box<KeyBuilder>,
    value: Box<dyn ValueBuilderImpl>,
) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: vec![key, value.into_operand_builder()],
        key_condition_list: Vec::new(),
        mode: KeyConditionMode::LessThan,
    }
}

pub fn key_less_than_equal(
    key: Box<KeyBuilder>,
    value: Box<dyn ValueBuilderImpl>,
) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: vec![key, value.into_operand_builder()],
        key_condition_list: Vec::new(),
        mode: KeyConditionMode::LessThanEqual,
    }
}

pub fn key_greater_than(
    key: Box<KeyBuilder>,
    value: Box<dyn ValueBuilderImpl>,
) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: vec![key, value.into_operand_builder()],
        key_condition_list: Vec::new(),
        mode: KeyConditionMode::GreaterThan,
    }
}

pub fn key_greater_than_equal(
    key: Box<KeyBuilder>,
    value: Box<dyn ValueBuilderImpl>,
) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: vec![key, value.into_operand_builder()],
        key_condition_list: Vec::new(),
        mode: KeyConditionMode::GreaterThanEqual,
    }
}

pub fn key_and(left: KeyConditionBuilder, right: KeyConditionBuilder) -> KeyConditionBuilder {
    if left.mode != KeyConditionMode::Equal {
        return KeyConditionBuilder {
            operand_list: Vec::new(),
            key_condition_list: Vec::new(),
            mode: KeyConditionMode::Invalid,
        };
    }

    if right.mode == KeyConditionMode::And {
        return KeyConditionBuilder {
            operand_list: Vec::new(),
            key_condition_list: Vec::new(),
            mode: KeyConditionMode::Invalid,
        };
    }

    KeyConditionBuilder {
        operand_list: Vec::new(),
        key_condition_list: vec![left, right],
        mode: KeyConditionMode::And,
    }
}

pub fn key_between(
    key: Box<KeyBuilder>,
    upper: Box<dyn ValueBuilderImpl>,
    lower: Box<dyn ValueBuilderImpl>,
) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: vec![
            key,
            upper.into_operand_builder(),
            lower.into_operand_builder(),
        ],
        key_condition_list: Vec::new(),
        mode: KeyConditionMode::Between,
    }
}

pub fn key_begins_with(key: Box<KeyBuilder>, prefix: impl Into<String>) -> KeyConditionBuilder {
    let v = value(prefix.into());
    KeyConditionBuilder {
        operand_list: vec![key, v],
        key_condition_list: Vec::new(),
        mode: KeyConditionMode::BeginsWith,
    }
}

impl KeyBuilder {
    pub fn equal(self: Box<KeyBuilder>, value: Box<dyn ValueBuilderImpl>) -> KeyConditionBuilder {
        key_equal(self, value)
    }

    pub fn less_than(
        self: Box<KeyBuilder>,
        value: Box<dyn ValueBuilderImpl>,
    ) -> KeyConditionBuilder {
        key_less_than(self, value)
    }

    pub fn less_than_equal(
        self: Box<KeyBuilder>,
        value: Box<dyn ValueBuilderImpl>,
    ) -> KeyConditionBuilder {
        key_less_than_equal(self, value)
    }

    pub fn greater_than(
        self: Box<KeyBuilder>,
        value: Box<dyn ValueBuilderImpl>,
    ) -> KeyConditionBuilder {
        key_greater_than(self, value)
    }

    pub fn greater_than_equal(
        self: Box<KeyBuilder>,
        value: Box<dyn ValueBuilderImpl>,
    ) -> KeyConditionBuilder {
        key_greater_than_equal(self, value)
    }

    pub fn between(
        self: Box<KeyBuilder>,
        upper: Box<dyn ValueBuilderImpl>,
        lower: Box<dyn ValueBuilderImpl>,
    ) -> KeyConditionBuilder {
        key_between(self, upper, lower)
    }

    pub fn begins_with(self: Box<KeyBuilder>, prefix: impl Into<String>) -> KeyConditionBuilder {
        key_begins_with(self, prefix)
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::AttributeValue;

    use crate::*;

    #[test]
    fn key_equal() -> anyhow::Result<()> {
        let input = key("foo").equal(value(5));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(vec![AttributeValue::N("5".to_owned())], "$v"),
                ],
                "$c = $c"
            )
        );

        Ok(())
    }

    #[test]
    fn key_less_than() -> anyhow::Result<()> {
        let input = key("foo").less_than(value(5));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(vec![AttributeValue::N("5".to_owned())], "$v"),
                ],
                "$c < $c"
            )
        );

        Ok(())
    }

    #[test]
    fn key_less_than_equal() -> anyhow::Result<()> {
        let input = key("foo").less_than_equal(value(5));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(vec![AttributeValue::N("5".to_owned())], "$v"),
                ],
                "$c <= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn key_greater_than() -> anyhow::Result<()> {
        let input = key("foo").greater_than(value(5));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(vec![AttributeValue::N("5".to_owned())], "$v"),
                ],
                "$c > $c"
            )
        );

        Ok(())
    }

    #[test]
    fn key_greater_than_equal() -> anyhow::Result<()> {
        let input = key("foo").greater_than_equal(value(5));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(vec![AttributeValue::N("5".to_owned())], "$v"),
                ],
                "$c >= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn unset_key_condition_builder() -> anyhow::Result<()> {
        let input = KeyConditionBuilder::default();

        assert_eq!(
            input
                .build_tree()
                .map_err(|e| e.downcast::<error::ExpressionError>().unwrap())
                .unwrap_err(),
            error::ExpressionError::UnsetParameterError(
                "buildTree".to_owned(),
                "KeyConditionBuilder".to_owned()
            )
        );

        Ok(())
    }

    #[test]
    fn key_between() -> anyhow::Result<()> {
        let input = key("foo").between(value(5), value(10));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(vec![AttributeValue::N("5".to_owned())], "$v"),
                    ExpressionNode::from_values(vec![AttributeValue::N("10".to_owned())], "$v"),
                ],
                "$c BETWEEN $c AND $c"
            )
        );

        Ok(())
    }

    #[test]
    fn key_begins_with() -> anyhow::Result<()> {
        let input = key("foo").begins_with("bar");

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(vec![AttributeValue::S("bar".to_owned())], "$v"),
                ],
                "begins_with ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn key_and() -> anyhow::Result<()> {
        let input = key("foo")
            .equal(value(5))
            .and(key("bar").begins_with("baz"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_children_expression(
                        vec![
                            ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                            ExpressionNode::from_values(
                                vec![AttributeValue::N("5".to_owned())],
                                "$v"
                            )
                        ],
                        "$c = $c",
                    ),
                    ExpressionNode::from_children_expression(
                        vec![
                            ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                            ExpressionNode::from_values(
                                vec![AttributeValue::S("baz".to_owned())],
                                "$v"
                            ),
                        ],
                        "begins_with ($c, $c)",
                    )
                ],
                "($c) AND ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn first_condition_not_equal() -> anyhow::Result<()> {
        let input = key("foo")
            .less_than(value(5))
            .and(key("bar").begins_with("baz"));

        assert_eq!(
            input.build_tree().unwrap_err().to_string(),
            "buildKeyCondition error: invalid key condition constructed"
        );

        Ok(())
    }

    #[test]
    fn more_than_one_condition() -> anyhow::Result<()> {
        let input = key("foo").equal(value(5)).and(
            key("bar")
                .equal(value(1))
                .and(key("baz").begins_with("yar")),
        );

        assert_eq!(
            input.build_tree().unwrap_err().to_string(),
            "buildKeyCondition error: invalid key condition constructed"
        );

        Ok(())
    }

    #[test]
    fn operand_error() -> anyhow::Result<()> {
        let input = key("").equal(value("yikes".to_owned()));

        assert_eq!(
            input
                .build_tree()
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
    fn build_child_nodes() -> anyhow::Result<()> {
        let input = key("foo")
            .equal(value("bar"))
            .and(key("baz").less_than(value(10)));

        assert_eq!(
            input.build_child_nodes()?,
            vec![
                ExpressionNode::from_children_expression(
                    vec![
                        ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                        ExpressionNode::from_values(
                            vec![AttributeValue::S("bar".to_owned())],
                            "$v"
                        )
                    ],
                    "$c = $c"
                ),
                ExpressionNode::from_children_expression(
                    vec![
                        ExpressionNode::from_names(vec!["baz".to_owned()], "$n"),
                        ExpressionNode::from_values(vec![AttributeValue::N("10".to_owned())], "$v")
                    ],
                    "$c < $c"
                ),
            ],
        );

        Ok(())
    }
}
