use anyhow::bail;

use crate::{error::ExpressionError, ExpressionNode, NameBuilder, OperandBuilder, TreeBuilder};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/projection.go

#[derive(Default)]
pub struct ProjectionBuilder {
    #[allow(clippy::vec_box)]
    names: Vec<Box<NameBuilder>>,
}

impl ProjectionBuilder {
    pub fn add_names(self, names_list: impl Into<Vec<Box<NameBuilder>>>) -> ProjectionBuilder {
        add_names(self, names_list)
    }

    fn build_child_nodes(&self) -> anyhow::Result<Vec<ExpressionNode>> {
        let mut child_nodes = Vec::new();
        for name in &self.names {
            let operand = name.build_operand()?;
            child_nodes.push(operand.expression_node);
        }
        Ok(child_nodes)
    }
}

impl TreeBuilder for ProjectionBuilder {
    fn build_tree(&self) -> anyhow::Result<ExpressionNode> {
        if self.names.is_empty() {
            bail!(ExpressionError::UnsetParameterError(
                "buildTree".to_owned(),
                "ProjectionBuilder".to_owned(),
            ));
        }

        let child_nodes = self.build_child_nodes()?;

        let node = ExpressionNode::from_children_expression(
            child_nodes,
            format!("$c{}", ", $c".repeat(self.names.len() - 1)),
        );

        Ok(node)
    }
}

#[allow(clippy::boxed_local)]
pub fn names_list(
    name_builder: Box<NameBuilder>,
    names_list: impl Into<Vec<Box<NameBuilder>>>,
) -> ProjectionBuilder {
    let mut names_list = names_list.into();

    names_list.insert(0, name_builder);
    ProjectionBuilder { names: names_list }
}

pub fn add_names(
    mut projection_builder: ProjectionBuilder,
    names_list: impl Into<Vec<Box<NameBuilder>>>,
) -> ProjectionBuilder {
    projection_builder.names.append(&mut names_list.into());

    projection_builder
}

impl NameBuilder {
    pub fn names_list(
        self: Box<NameBuilder>,
        names: impl Into<Vec<Box<NameBuilder>>>,
    ) -> ProjectionBuilder {
        names_list(self, names)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn names_list_function_call() -> anyhow::Result<()> {
        let input = names_list(name("foo"), vec![name("bar")]);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n")
                ],
                "$c, $c"
            )
        );

        Ok(())
    }

    #[test]
    fn names_list_method_call() -> anyhow::Result<()> {
        let input = name("foo").names_list(vec![name("bar")]);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c, $c"
            )
        );

        Ok(())
    }

    #[test]
    fn add_name() -> anyhow::Result<()> {
        let input = name("foo")
            .names_list(vec![name("bar")])
            .add_names(vec![name("baz"), name("qux")]);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["baz".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["qux".to_owned()], "$n"),
                ],
                "$c, $c, $c, $c"
            )
        );

        Ok(())
    }

    #[test]
    fn build_projection_3() -> anyhow::Result<()> {
        let input = names_list(name("foo"), vec![name("bar"), name("baz")]);

        assert_eq!(input.build_tree()?.fmt_expression, "$c, $c, $c");

        Ok(())
    }

    #[test]
    fn build_projection_5() -> anyhow::Result<()> {
        let input = ProjectionBuilder::default();

        assert_eq!(
            input
                .build_tree()
                .map_err(|e| e.downcast::<error::ExpressionError>().unwrap())
                .unwrap_err(),
            error::ExpressionError::UnsetParameterError(
                "buildTree".to_owned(),
                "ProjectionBuilder".to_owned()
            )
        );

        Ok(())
    }

    #[test]
    fn build_child_nodes() -> anyhow::Result<()> {
        let input = names_list(name("foo"), vec![name("bar"), name("baz")]);

        assert_eq!(
            input.build_tree()?.children,
            vec![
                ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ExpressionNode::from_names(vec!["baz".to_owned()], "$n"),
            ],
        );

        Ok(())
    }

    #[test]
    fn operand_error() -> anyhow::Result<()> {
        let input = names_list(name(""), vec![]);

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
}
