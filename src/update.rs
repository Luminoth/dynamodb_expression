use anyhow::bail;

use std::collections::HashMap;

use crate::{ExpressionNode, NameBuilder, OperandBuilder, TreeBuilder};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/update.go

#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)]
enum OperationMode {
    Set,
    Remove,
    Add,
    Delete,
}

impl OperationMode {
    pub fn as_str(&self) -> &str {
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

impl OperationBuilder {
    fn build_operation(&self) -> anyhow::Result<ExpressionNode> {
        unimplemented!("OperationBuilder::build_operation");
    }

    fn build_child_nodes(
        operation_builder_list: &Vec<OperationBuilder>,
    ) -> anyhow::Result<ExpressionNode> {
        if operation_builder_list.len() == 0 {
            bail!("buildChildNodes error: operationBuilder list is empty");
        }

        let mut node = ExpressionNode::default();
        node.fmt_expression = format!("$c{}", ", $c".repeat(operation_builder_list.len() - 1));

        for val in operation_builder_list {
            let val_node = val.build_operation()?;
            node.children.push(val_node);
        }

        Ok(node)
    }
}

pub struct UpdateBuilder {
    operations: HashMap<OperationMode, Vec<OperationBuilder>>,
}

impl TreeBuilder for UpdateBuilder {
    fn build_tree(&self) -> anyhow::Result<ExpressionNode> {
        let mut ret = ExpressionNode::default();

        let mut modes = Vec::new();
        for mode in self.operations.keys() {
            modes.push(mode);
        }
        modes.sort();

        for key in modes {
            ret.fmt_expression
                .push_str(&format!("{} $c\n", key.as_str()));

            let child_node =
                OperationBuilder::build_child_nodes(self.operations.get(key).unwrap())?;
            ret.children.push(child_node);
        }

        Ok(ret)
    }
}
