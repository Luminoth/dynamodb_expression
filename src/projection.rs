use anyhow::bail;

use crate::{ExpressionNode, NameBuilder, OperandBuilder, TreeBuilder};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/projection.go

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
            bail!("ProjectionBuilder build_tree");
        }

        let child_nodes = self.build_child_nodes()?;

        let mut node = ExpressionNode::from_children(child_nodes);
        node.fmt_expression = format!("$c{}", ", $c".repeat(self.names.len() - 1));

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
