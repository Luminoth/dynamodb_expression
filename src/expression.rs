use std::collections::HashMap;

use anyhow::bail;
use rusoto_dynamodb::AttributeValue;

use crate::{ConditionBuilder, KeyConditionBuilder, ProjectionBuilder, UpdateBuilder};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/expression.go

#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)]
enum ExpressionType {
    Projection,
    KeyCondition,
    Condition,
    Filter,
    Update,
}

#[derive(Default)]
pub struct Expression {
    expressions: HashMap<ExpressionType, String>,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
}

impl Expression {
    fn new(expressions: HashMap<ExpressionType, String>) -> Self {
        Self {
            expressions,
            ..Default::default()
        }
    }

    pub fn condition(&self) -> Option<&String> {
        self.return_expression(ExpressionType::Condition)
    }

    pub fn filter(&self) -> Option<&String> {
        self.return_expression(ExpressionType::Filter)
    }

    pub fn projection(&self) -> Option<&String> {
        self.return_expression(ExpressionType::Projection)
    }

    pub fn key_condition(&self) -> Option<&String> {
        self.return_expression(ExpressionType::KeyCondition)
    }

    pub fn update(&self) -> Option<&String> {
        self.return_expression(ExpressionType::Update)
    }

    pub fn names(&self) -> &HashMap<String, String> {
        &self.names
    }

    pub fn values(&self) -> &HashMap<String, AttributeValue> {
        &self.values
    }

    fn return_expression(&self, expression_type: ExpressionType) -> Option<&String> {
        self.expressions.get(&expression_type)
    }
}

#[derive(Default)]
pub struct Builder {
    expressions: HashMap<ExpressionType, Box<dyn TreeBuilder>>,
}

impl Builder {
    pub fn with_condition(mut self, condition_builder: ConditionBuilder) -> Builder {
        self.expressions
            .insert(ExpressionType::Condition, Box::new(condition_builder));

        self
    }

    pub fn with_projection(mut self, projection_builder: ProjectionBuilder) -> Builder {
        self.expressions
            .insert(ExpressionType::Projection, Box::new(projection_builder));

        self
    }

    pub fn with_key_condition(mut self, key_condition_builder: KeyConditionBuilder) -> Builder {
        self.expressions.insert(
            ExpressionType::KeyCondition,
            Box::new(key_condition_builder),
        );

        self
    }

    pub fn with_filter(mut self, filter: ConditionBuilder) -> Builder {
        self.expressions
            .insert(ExpressionType::Filter, Box::new(filter));

        self
    }

    pub fn with_update(mut self, update_builder: UpdateBuilder) -> Builder {
        self.expressions
            .insert(ExpressionType::Update, Box::new(update_builder));

        self
    }

    pub fn build(&self) -> anyhow::Result<Expression> {
        let (alias_list, expressions) = self.build_child_trees()?;

        let mut expression = Expression::new(expressions);

        if !alias_list.names.is_empty() {
            let mut names = HashMap::new();
            for (ind, val) in alias_list.names.iter().enumerate() {
                names.insert(format!("#{}", ind), val.clone());
            }
            expression.names = names;
        }

        if !alias_list.values.is_empty() {
            let mut values = HashMap::new();
            for (ind, val) in alias_list.values.iter().enumerate() {
                values.insert(format!(":{}", ind), val.clone());
            }
            expression.values = values;
        }

        Ok(expression)
    }

    fn build_child_trees(&self) -> anyhow::Result<(AliasList, HashMap<ExpressionType, String>)> {
        let mut alias_list = AliasList::default();
        let mut formatted_expressions = HashMap::new();
        let mut keys = Vec::new();

        for expression_type in self.expressions.keys() {
            keys.push(*expression_type);
        }
        keys.sort();

        for key in keys.iter() {
            let node = self.expressions[key].build_tree()?;
            let formatted_expression = node.build_expression_string(&mut alias_list)?;
            formatted_expressions.insert(*key, formatted_expression);
        }

        Ok((alias_list, formatted_expressions))
    }
}

#[derive(Default)]
struct AliasList {
    names: Vec<String>,
    values: Vec<AttributeValue>,
}

impl AliasList {
    fn alias_value(&mut self, dav: AttributeValue) -> anyhow::Result<String> {
        self.values.push(dav);
        Ok(format!(":{}", self.values.len() - 1))
    }

    fn alias_path(&mut self, nm: impl Into<String>) -> anyhow::Result<String> {
        let nm = nm.into();

        for (idx, name) in self.names.iter().enumerate() {
            if nm == *name {
                return Ok(format!("#{}", idx));
            }
        }

        self.names.push(nm);
        Ok(format!("#{}", self.names.len() - 1))
    }
}

pub(crate) trait TreeBuilder {
    fn build_tree(&self) -> anyhow::Result<ExpressionNode>;
}

#[derive(Default, Debug, PartialEq)]
pub(crate) struct ExpressionNode {
    pub(crate) names: Vec<String>,
    values: Vec<AttributeValue>,
    pub(crate) children: Vec<ExpressionNode>,
    pub(crate) fmt_expression: String,
}

impl ExpressionNode {
    pub(crate) fn from_names(names: Vec<String>, fmt_exression: impl Into<String>) -> Self {
        Self {
            names,
            fmt_expression: fmt_exression.into(),
            ..Default::default()
        }
    }

    pub(crate) fn from_values(
        values: Vec<AttributeValue>,
        fmt_exression: impl Into<String>,
    ) -> Self {
        Self {
            values,
            fmt_expression: fmt_exression.into(),
            ..Default::default()
        }
    }

    pub(crate) fn from_children(children: Vec<ExpressionNode>) -> Self {
        Self {
            children,
            ..Default::default()
        }
    }

    pub(crate) fn from_children_expression(
        children: Vec<ExpressionNode>,
        fmt_expression: impl Into<String>,
    ) -> Self {
        Self {
            children,
            fmt_expression: fmt_expression.into(),
            ..Default::default()
        }
    }

    fn build_expression_string(&self, alias_list: &mut AliasList) -> anyhow::Result<String> {
        // Since each exprNode contains a slice of names, values, and children that
        // correspond to the escaped characters, we an index to traverse the slices
        let mut index = (0, 0, 0);

        let mut formatted_expression = self.fmt_expression.clone();

        let mut idx = 0;
        while idx < formatted_expression.len() {
            if formatted_expression.chars().nth(idx).unwrap() != '$' {
                idx += 1;
                continue;
            }

            if idx == formatted_expression.len() - 1 {
                bail!("build_expression_string error: invalid escape character");
            }

            // if an escaped character is found, substitute it with the proper alias
            // TODO consider AST instead of string in the future
            let rune = formatted_expression.chars().nth(idx + 1).unwrap();
            let alias = match rune {
                'n' => {
                    let alias = self.substitute_path(index.0, alias_list)?;
                    index.0 += 1;
                    alias
                }
                'v' => {
                    let alias = self.substitute_value(index.1, alias_list)?;
                    index.1 += 1;
                    alias
                }
                'c' => {
                    let alias = self.substitute_child(index.1, alias_list)?;
                    index.2 += 1;
                    alias
                }
                _ => bail!(
                    "build_expression_string error: invalid escape rune {}",
                    rune
                ),
            };

            formatted_expression = format!(
                "{}{}{}",
                &formatted_expression.as_str()[..1],
                alias,
                &formatted_expression.as_str()[idx + 2..]
            );
            idx += alias.len();
        }

        Ok(formatted_expression)
    }

    fn substitute_path(&self, index: usize, alias_list: &mut AliasList) -> anyhow::Result<String> {
        if index >= self.names.len() {
            bail!("substitute_path error: ExpressionNode names out of range");
        }
        alias_list.alias_path(self.names[index].clone())
    }

    fn substitute_value(&self, index: usize, alias_list: &mut AliasList) -> anyhow::Result<String> {
        if index >= self.values.len() {
            bail!("substitute_path error: ExpressionNode values out of range");
        }
        alias_list.alias_value(self.values[index].clone())
    }

    fn substitute_child(&self, index: usize, alias_list: &mut AliasList) -> anyhow::Result<String> {
        if index >= self.children.len() {
            bail!("substitute_path error: ExpressionNode children out of range");
        }
        self.children[index].build_expression_string(alias_list)
    }
}
