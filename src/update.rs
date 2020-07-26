use std::collections::HashMap;

use crate::{ExpressionNode, NameBuilder, OperandBuilder, TreeBuilder};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/update.go

#[derive(Copy, Clone, PartialEq, Debug)]
enum OperationMode {
    Set,
    Remove,
    Add,
    Delete,
}

impl OperationMode {
    pub fn to_string(&self) -> &str {
        match self {
            OperationMode::Set => "SET",
            OperationMode::Remove => "REMOVE",
            OperationMode::Add => "ADD",
            OperationMode::Delete => "DELETE",
        }
    }
}

struct OperationBuilder {
    name: Box<NameBuilder>,
    value: Box<dyn OperandBuilder>,
    mode: OperationMode,
}

pub struct UpdateBuilder {
    operations: HashMap<OperationMode, Vec<OperationBuilder>>,
}

impl TreeBuilder for UpdateBuilder {
    fn build_tree(&self) -> anyhow::Result<ExpressionNode> {
        unimplemented!("UpdateBuilder::build_tree")
    }
}
