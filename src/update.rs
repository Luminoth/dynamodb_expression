use anyhow::bail;

use std::collections::HashMap;

use crate::{ExpressionNode, NameBuilder, OperandBuilder, TreeBuilder, ValueBuilderImpl};

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
    value: Option<Box<dyn OperandBuilder>>,
    mode: OperationMode,
}

impl OperationBuilder {
    fn build_operation(&self) -> anyhow::Result<ExpressionNode> {
        let path_child = self.name.build_operand()?;

        let mut node = ExpressionNode::from_children_expression(
            vec![path_child.expression_node],
            "$c".to_owned(),
        );

        if self.mode == OperationMode::Remove {
            return Ok(node);
        }

        if let Some(value) = &self.value {
            let value_child = value.build_operand()?;
            node.children.push(value_child.expression_node);
        }

        node.fmt_expression.push_str(match self.mode {
            OperationMode::Set => " = $c",
            OperationMode::Add => " $c",
            _ => bail!(
                "build update error: build operation error: unsupported mode: {:?}",
                self.mode
            ),
        });

        Ok(node)
    }

    fn build_child_nodes(
        operation_builder_list: impl AsRef<[OperationBuilder]>,
    ) -> anyhow::Result<ExpressionNode> {
        if operation_builder_list.as_ref().is_empty() {
            bail!("buildChildNodes error: operationBuilder list is empty");
        }

        let mut node = ExpressionNode::default();
        node.fmt_expression = format!(
            "$c{}",
            ", $c".repeat(operation_builder_list.as_ref().len() - 1)
        );

        for val in operation_builder_list.as_ref() {
            let val_node = val.build_operation()?;
            node.children.push(val_node);
        }

        Ok(node)
    }
}

pub fn delete(name: Box<NameBuilder>, value: Box<dyn ValueBuilderImpl>) -> UpdateBuilder {
    let empty_update_builder = UpdateBuilder {
        operations: HashMap::new(),
    };
    empty_update_builder.delete(name, value)
}

pub fn add(name: Box<NameBuilder>, value: Box<dyn ValueBuilderImpl>) -> UpdateBuilder {
    let empty_update_builder = UpdateBuilder {
        operations: HashMap::new(),
    };
    empty_update_builder.add(name, value)
}

pub fn remove(name: Box<NameBuilder>) -> UpdateBuilder {
    let empty_update_builder = UpdateBuilder {
        operations: HashMap::new(),
    };
    empty_update_builder.remove(name)
}

pub fn set(name: Box<NameBuilder>, operand_builder: Box<dyn OperandBuilder>) -> UpdateBuilder {
    let empty_update_builder = UpdateBuilder {
        operations: HashMap::new(),
    };
    empty_update_builder.set(name, operand_builder)
}

pub struct UpdateBuilder {
    operations: HashMap<OperationMode, Vec<OperationBuilder>>,
}

impl UpdateBuilder {
    pub fn delete(
        mut self,
        name: Box<NameBuilder>,
        value: Box<dyn ValueBuilderImpl>,
    ) -> UpdateBuilder {
        self.operations
            .entry(OperationMode::Delete)
            .or_insert_with(Vec::new)
            .push(OperationBuilder {
                name,
                value: Some(value.into_operand_builder()),
                mode: OperationMode::Delete,
            });

        self
    }

    pub fn add(
        mut self,
        name: Box<NameBuilder>,
        value: Box<dyn ValueBuilderImpl>,
    ) -> UpdateBuilder {
        self.operations
            .entry(OperationMode::Add)
            .or_insert_with(Vec::new)
            .push(OperationBuilder {
                name,
                value: Some(value.into_operand_builder()),
                mode: OperationMode::Add,
            });

        self
    }

    pub fn remove(mut self, name: Box<NameBuilder>) -> UpdateBuilder {
        self.operations
            .entry(OperationMode::Remove)
            .or_insert_with(Vec::new)
            .push(OperationBuilder {
                name,
                value: None,
                mode: OperationMode::Remove,
            });

        self
    }

    pub fn set(
        mut self,
        name: Box<NameBuilder>,
        operand_builder: Box<dyn OperandBuilder>,
    ) -> UpdateBuilder {
        self.operations
            .entry(OperationMode::Set)
            .or_insert_with(Vec::new)
            .push(OperationBuilder {
                name,
                value: Some(operand_builder),
                mode: OperationMode::Set,
            });

        self
    }
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
