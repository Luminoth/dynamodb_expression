use crate::{ExpressionNode, NameBuilder, TreeBuilder};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/projection.go

pub struct ProjectionBuilder {
    names: Vec<Box<NameBuilder>>,
}

impl ProjectionBuilder {
    pub fn add_names(self, names_list: impl Into<Vec<Box<NameBuilder>>>) -> ProjectionBuilder {
        add_names(self, names_list)
    }
}

impl TreeBuilder for ProjectionBuilder {
    fn build_tree(&self) -> anyhow::Result<ExpressionNode> {
        unimplemented!("ConditionBuilder::build_tree")
    }
}

pub fn names_list(
    names_builder: Box<NameBuilder>,
    names_list: impl Into<Vec<Box<NameBuilder>>>,
) -> ProjectionBuilder {
    ProjectionBuilder {
        names: names_list.into(),
    }
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
