use std::collections::HashMap;

use anyhow::bail;
use rusoto_dynamodb::AttributeValue;

use crate::{ConditionBuilder, KeyConditionBuilder, ProjectionBuilder, UpdateBuilder};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/expression.go

#[derive(Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug)]
pub(crate) enum ExpressionType {
    Projection,
    KeyCondition,
    Condition,
    Filter,
    Update,
}

#[derive(Default, Debug, PartialEq)]
pub struct Expression {
    expressions: HashMap<ExpressionType, String>,
    names: Option<HashMap<String, String>>,
    values: Option<HashMap<String, AttributeValue>>,
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

    pub fn names(&self) -> &Option<HashMap<String, String>> {
        &self.names
    }

    pub fn values(&self) -> &Option<HashMap<String, AttributeValue>> {
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
    // TODO: this isn't really needed
    pub fn new() -> Self {
        Self {
            expressions: HashMap::new(),
        }
    }

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

    pub fn build(self) -> anyhow::Result<Expression> {
        let (alias_list, expressions) = self.build_child_trees()?;

        let mut expression = Expression::new(expressions);

        if !alias_list.names.is_empty() {
            let mut names = HashMap::new();
            for (ind, val) in alias_list.names.iter().enumerate() {
                names.insert(format!("#{}", ind), val.clone());
            }
            expression.names = Some(names);
        }

        if !alias_list.values.is_empty() {
            let mut values = HashMap::new();
            for (ind, val) in alias_list.values.iter().enumerate() {
                values.insert(format!(":{}", ind), val.clone());
            }
            expression.values = Some(values);
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

pub(crate) trait TreeBuilder: Send {
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
                bail!("buildexprNode error: invalid escape character");
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
                    let alias = self.substitute_child(index.2, alias_list)?;
                    index.2 += 1;
                    alias
                }
                _ => bail!("buildexprNode error: invalid escape rune {}", rune),
            };

            formatted_expression = format!(
                "{}{}{}",
                &formatted_expression.as_str()[..idx],
                alias,
                &formatted_expression.as_str()[idx + 2..]
            );
            idx += alias.len();
        }

        Ok(formatted_expression)
    }

    fn substitute_path(&self, index: usize, alias_list: &mut AliasList) -> anyhow::Result<String> {
        if index >= self.names.len() {
            bail!("substitutePath error: exprNode []names out of range");
        }
        alias_list.alias_path(self.names[index].clone())
    }

    fn substitute_value(&self, index: usize, alias_list: &mut AliasList) -> anyhow::Result<String> {
        if index >= self.values.len() {
            bail!("substituteValue error: exprNode []values out of range");
        }
        alias_list.alias_value(self.values[index].clone())
    }

    fn substitute_child(&self, index: usize, alias_list: &mut AliasList) -> anyhow::Result<String> {
        if index >= self.children.len() {
            bail!("substituteChild error: exprNode []children out of range");
        }
        self.children[index].build_expression_string(alias_list)
    }
}

#[cfg(test)]
mod tests {
    use rusoto_dynamodb::AttributeValue;

    use crate::*;

    //https://stackoverflow.com/questions/27582739/how-do-i-create-a-hashmap-literal
    macro_rules! hashmap(
        { $($key:expr => $value:expr),+ } => {
            {
                let mut m = ::std::collections::HashMap::new();
                $(
                    m.insert($key, $value);
                )+
                m
            }
        };
    );

    #[test]
    fn condition() -> anyhow::Result<()> {
        let input = Builder::new().with_condition(name("foo").equal(value(5)));

        assert_eq!(
            input.build()?,
            Expression {
                expressions: hashmap!(ExpressionType::Condition => "#0 = :0".to_owned()),
                names: Some(hashmap!("#0".to_owned() => "foo".to_owned())),
                values: Some(hashmap!(":0".to_owned() => AttributeValue{
                    n: Some("5".to_owned()),
                    ..Default::default()
                })),
            },
        );

        Ok(())
    }

    #[test]
    fn projection() -> anyhow::Result<()> {
        let input =
            Builder::new().with_projection(names_list(name("foo"), vec![name("bar"), name("baz")]));

        assert_eq!(
            input.build()?,
            Expression {
                expressions: hashmap!(ExpressionType::Projection => "#0, #1, #2".to_owned()),
                names: Some(
                    hashmap!("#0".to_owned() => "foo".to_owned(), "#1".to_owned() => "bar".to_owned(), "#2".to_owned() => "baz".to_owned())
                ),
                ..Default::default()
            },
        );

        Ok(())
    }

    #[test]
    fn key_condition() -> anyhow::Result<()> {
        let input = Builder::new().with_key_condition(key("foo").equal(value(5)));

        assert_eq!(
            input.build()?,
            Expression {
                expressions: hashmap!(ExpressionType::KeyCondition => "#0 = :0".to_owned()),
                names: Some(hashmap!("#0".to_owned() => "foo".to_owned())),
                values: Some(hashmap!(":0".to_owned() => AttributeValue{
                    n: Some("5".to_owned()),
                    ..Default::default()
                })),
            },
        );

        Ok(())
    }

    #[test]
    fn filter() -> anyhow::Result<()> {
        let input = Builder::new().with_filter(name("foo").equal(value(5)));

        assert_eq!(
            input.build()?,
            Expression {
                expressions: hashmap!(ExpressionType::Filter => "#0 = :0".to_owned()),
                names: Some(hashmap!("#0".to_owned() => "foo".to_owned())),
                values: Some(hashmap!(":0".to_owned() => AttributeValue{
                    n: Some("5".to_owned()),
                    ..Default::default()
                })),
            },
        );

        Ok(())
    }

    #[test]
    fn update() -> anyhow::Result<()> {
        let input = Builder::new().with_update(set(name("foo"), value(5)));

        assert_eq!(
            input.build()?,
            Expression {
                expressions: hashmap!(ExpressionType::Update => "SET #0 = :0\n".to_owned()),
                names: Some(hashmap!("#0".to_owned() => "foo".to_owned())),
                values: Some(hashmap!(":0".to_owned() => AttributeValue{
                    n: Some("5".to_owned()),
                    ..Default::default()
                })),
            },
        );

        Ok(())
    }

    // TODO: not sure if it matters, but this test produces
    // different results than the Go version, however the
    // end dynamo outcome is the same for both
    #[test]
    fn compound() -> anyhow::Result<()> {
        let input = Builder::new()
            .with_condition(name("foo").equal(value(5)))
            .with_filter(name("bar").less_than(value(6)))
            .with_projection(names_list(name("foo"), vec![name("bar"), name("baz")]))
            .with_key_condition(key("foo").equal(value(5)))
            .with_update(set(name("foo"), value(5)));

        assert_eq!(
            input.build()?,
            Expression {
                expressions: hashmap!(
                ExpressionType::Condition => "#0 = :1".to_owned(),
                ExpressionType::Filter => "#1 < :2".to_owned(),
                ExpressionType::Projection => "#0, #1, #2".to_owned(),
                ExpressionType::KeyCondition => "#0 = :0".to_owned(),
                ExpressionType::Update => "SET #0 = :3\n".to_owned()
                ),
                names: Some(hashmap!(
                "#0".to_owned() => "foo".to_owned(),
                "#1".to_owned() => "bar".to_owned(),
                "#2".to_owned() => "baz".to_owned()
                )),
                values: Some(hashmap!(
                    ":0".to_owned() => AttributeValue{
                        n: Some("5".to_owned()),
                        ..Default::default()
                    },
                    ":1".to_owned() => AttributeValue{
                        n: Some("5".to_owned()),
                        ..Default::default()
                    },
                    ":2".to_owned() => AttributeValue{
                        n: Some("6".to_owned()),
                        ..Default::default()
                    },
                    ":3".to_owned() => AttributeValue{
                        n: Some("5".to_owned()),
                        ..Default::default()
                    }
                )),
            },
        );

        Ok(())
    }

    #[test]
    fn invalid_builder() -> anyhow::Result<()> {
        let input = Builder::new().with_condition(name("").equal(value(5)));

        assert_eq!(
            input
                .build()
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
    fn test_condition() -> anyhow::Result<()> {
        let input = Builder::new().with_condition(name("foo").equal(value(5)));

        assert_eq!(*input.build()?.condition().unwrap(), "#0 = :0".to_owned(),);

        Ok(())
    }

    #[test]
    fn test_condition_unset() -> anyhow::Result<()> {
        let input = Builder::new();

        assert_eq!(input.build()?.condition(), None);

        Ok(())
    }

    #[test]
    fn test_filter() -> anyhow::Result<()> {
        let input = Builder::new().with_filter(name("foo").equal(value(5)));

        assert_eq!(*input.build()?.filter().unwrap(), "#0 = :0".to_owned(),);

        Ok(())
    }

    #[test]
    fn test_filter_unset() -> anyhow::Result<()> {
        let input = Builder::new();

        assert_eq!(input.build()?.filter(), None);

        Ok(())
    }

    #[test]
    fn test_projection() -> anyhow::Result<()> {
        let input =
            Builder::new().with_projection(names_list(name("foo"), vec![name("bar"), name("baz")]));

        assert_eq!(
            *input.build()?.projection().unwrap(),
            "#0, #1, #2".to_owned(),
        );

        Ok(())
    }

    #[test]
    fn test_projection_unset() -> anyhow::Result<()> {
        let input = Builder::new();

        assert_eq!(input.build()?.projection(), None);

        Ok(())
    }

    #[test]
    fn test_key_condition() -> anyhow::Result<()> {
        let input = Builder::new().with_key_condition(key("foo").equal(value(5)));

        assert_eq!(
            *input.build()?.key_condition().unwrap(),
            "#0 = :0".to_owned(),
        );

        Ok(())
    }

    #[test]
    fn test_key_condition_unset() -> anyhow::Result<()> {
        let input = Builder::new();

        assert_eq!(input.build()?.key_condition(), None);

        Ok(())
    }

    #[test]
    fn test_update() -> anyhow::Result<()> {
        let input = Builder::new().with_update(set(name("foo"), value(5)));

        assert_eq!(
            *input.build()?.update().unwrap(),
            "SET #0 = :0\n".to_owned(),
        );

        Ok(())
    }

    #[test]
    fn test_update_multiple_sets() -> anyhow::Result<()> {
        let input = Builder::new().with_update(
            set(name("foo"), value(5))
                .set(name("bar"), value(6))
                .set(name("baz"), value(7)),
        );

        assert_eq!(
            *input.build()?.update().unwrap(),
            "SET #0 = :0, #1 = :1, #2 = :2\n".to_owned(),
        );

        Ok(())
    }

    #[test]
    fn test_update_unset() -> anyhow::Result<()> {
        let input = Builder::new();

        assert_eq!(input.build()?.update(), None);

        Ok(())
    }

    #[test]
    fn names_projection() -> anyhow::Result<()> {
        let input =
            Builder::new().with_projection(names_list(name("foo"), vec![name("bar"), name("baz")]));

        assert_eq!(
            *input.build()?.names(),
            Some(hashmap!(
                "#0".to_owned() => "foo".to_owned(),
                "#1".to_owned() => "bar".to_owned(),
                "#2".to_owned() => "baz".to_owned()
            ))
        );

        Ok(())
    }

    #[test]
    fn names_aggregate() -> anyhow::Result<()> {
        let input = Builder::new()
            .with_condition(name("foo").equal(value(5)))
            .with_filter(name("bar").equal(value(6)))
            .with_projection(names_list(name("foo"), vec![name("bar"), name("baz")]));

        assert_eq!(
            *input.build()?.names(),
            Some(hashmap!(
                "#0".to_owned() => "foo".to_owned(),
                "#1".to_owned() => "bar".to_owned(),
                "#2".to_owned() => "baz".to_owned()
            ))
        );

        Ok(())
    }

    #[test]
    fn names_unset() -> anyhow::Result<()> {
        let input = Builder::new().with_condition(ConditionBuilder::default());

        assert_eq!(
            input
                .build()
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
    fn empty_string_sets_become_null() -> anyhow::Result<()> {
        let input =
            Builder::new().with_condition(name("groups").equal(value(Vec::<String>::new())));

        assert_eq!(
            *input.build()?.values(),
            Some(hashmap!(
                ":0".to_owned() => AttributeValue{
                    null: Some(true),
                    ..Default::default()
                }
            ))
        );

        Ok(())
    }

    #[test]
    fn empty_lists_become_null() -> anyhow::Result<()> {
        let input = Builder::new()
            .with_condition(name("groups").equal(value(Vec::<Box<dyn ValueBuilderImpl>>::new())));

        assert_eq!(
            *input.build()?.values(),
            Some(hashmap!(
                ":0".to_owned() => AttributeValue{
                    null: Some(true),
                    ..Default::default()
                }
            ))
        );

        Ok(())
    }

    #[test]
    fn attribute_value_used_directly() -> anyhow::Result<()> {
        let input = Builder::new().with_condition(name("key").equal(value(AttributeValue {
            s: Some("value".to_owned()),
            ..Default::default()
        })));

        assert_eq!(
            *input.build()?.values(),
            Some(hashmap!(
                ":0".to_owned() => AttributeValue{
                    s: Some("value".to_owned()),
                    ..Default::default()
                }
            ))
        );

        Ok(())
    }

    #[test]
    fn values_condition() -> anyhow::Result<()> {
        let input = Builder::new().with_condition(name("foo").equal(value(5)));

        assert_eq!(
            *input.build()?.values(),
            Some(hashmap!(
                ":0".to_owned() => AttributeValue{
                    n: Some("5".to_owned()),
                    ..Default::default()
                }
            ))
        );

        Ok(())
    }

    #[test]
    fn values_aggregate() -> anyhow::Result<()> {
        let input = Builder::new()
            .with_condition(name("foo").equal(value(5)))
            .with_filter(name("bar").less_than(value(6)))
            .with_projection(names_list(name("foo"), vec![name("bar"), name("baz")]));

        assert_eq!(
            *input.build()?.values(),
            Some(hashmap!(
                ":0".to_owned() => AttributeValue{
                    n: Some("5".to_owned()),
                    ..Default::default()
                },
                ":1".to_owned() => AttributeValue{
                    n: Some("6".to_owned()),
                    ..Default::default()
                }
            ))
        );

        Ok(())
    }

    #[test]
    fn values_unset() -> anyhow::Result<()> {
        let input = Builder::new();

        assert_eq!(*input.build()?.values(), None);

        Ok(())
    }

    #[test]
    fn basic_name() -> anyhow::Result<()> {
        let input = ExpressionNode::from_names(vec!["foo".to_owned()], "$n");

        assert_eq!(
            input.build_expression_string(&mut expression::AliasList::default())?,
            "#0"
        );

        Ok(())
    }

    #[test]
    fn basic_values() -> anyhow::Result<()> {
        let input = ExpressionNode::from_values(
            vec![AttributeValue {
                n: Some("5".to_owned()),
                ..Default::default()
            }],
            "$v".to_owned(),
        );

        assert_eq!(
            input.build_expression_string(&mut expression::AliasList::default())?,
            ":0"
        );

        Ok(())
    }

    #[test]
    fn nested_path() -> anyhow::Result<()> {
        let input = ExpressionNode::from_names(vec!["foo".to_owned(), "bar".to_owned()], "$n.$n");

        assert_eq!(
            input.build_expression_string(&mut expression::AliasList::default())?,
            "#0.#1"
        );

        Ok(())
    }

    #[test]
    fn nested_path_with_index() -> anyhow::Result<()> {
        let input = ExpressionNode::from_names(
            vec!["foo".to_owned(), "bar".to_owned(), "baz".to_owned()],
            "$n.$n[0].$n",
        );

        assert_eq!(
            input.build_expression_string(&mut expression::AliasList::default())?,
            "#0.#1[0].#2"
        );

        Ok(())
    }

    #[test]
    fn basic_size() -> anyhow::Result<()> {
        let input = ExpressionNode::from_names(vec!["foo".to_owned(), "foo".to_owned()], "$n.$n");

        assert_eq!(
            input.build_expression_string(&mut expression::AliasList::default())?,
            "#0.#0"
        );

        Ok(())
    }

    #[test]
    fn equal_expression() -> anyhow::Result<()> {
        let node = ExpressionNode::from_children_expression(
            vec![
                ExpressionNode::from_names(vec!["foo".to_string()], "$n"),
                ExpressionNode::from_values(
                    vec![AttributeValue {
                        n: Some("5".to_owned()),
                        ..Default::default()
                    }],
                    "$v",
                ),
            ],
            "$c = $c",
        );

        assert_eq!(
            node.build_expression_string(&mut expression::AliasList::default())?,
            "#0 = :0"
        );

        Ok(())
    }

    #[test]
    fn missing_char() -> anyhow::Result<()> {
        let input = ExpressionNode::from_names(vec!["foo".to_owned()], "$n.$");

        assert_eq!(
            input
                .build_expression_string(&mut expression::AliasList::default())
                .unwrap_err()
                .to_string(),
            "buildexprNode error: invalid escape character",
        );

        Ok(())
    }

    #[test]
    fn names_out_of_range() -> anyhow::Result<()> {
        let input = ExpressionNode::from_names(vec!["foo".to_owned()], "$n.$n");

        assert_eq!(
            input
                .build_expression_string(&mut expression::AliasList::default())
                .unwrap_err()
                .to_string(),
            "substitutePath error: exprNode []names out of range",
        );

        Ok(())
    }

    #[test]
    fn values_out_of_range() -> anyhow::Result<()> {
        let input = ExpressionNode::from_values(vec![], "$v");

        assert_eq!(
            input
                .build_expression_string(&mut expression::AliasList::default())
                .unwrap_err()
                .to_string(),
            "substituteValue error: exprNode []values out of range",
        );

        Ok(())
    }

    #[test]
    fn childre_out_of_range() -> anyhow::Result<()> {
        let input = ExpressionNode {
            fmt_expression: "$!".to_owned(),
            ..Default::default()
        };

        assert_eq!(
            input
                .build_expression_string(&mut expression::AliasList::default())
                .unwrap_err()
                .to_string(),
            "buildexprNode error: invalid escape rune !",
        );

        Ok(())
    }

    #[test]
    fn unset_expression_node() -> anyhow::Result<()> {
        let input = ExpressionNode::default();

        assert_eq!(
            input.build_expression_string(&mut expression::AliasList::default())?,
            "".to_owned(),
        );

        Ok(())
    }

    #[test]
    fn projection_exists() -> anyhow::Result<()> {
        let input = Expression::new(hashmap!(
            ExpressionType::Projection => "#0, #1, #2".to_owned()
        ));

        assert_eq!(
            input.return_expression(ExpressionType::Projection),
            Some(&"#0, #1, #2".to_owned()),
        );

        Ok(())
    }

    #[test]
    fn projection_not_exists() -> anyhow::Result<()> {
        let input = Expression::default();

        assert_eq!(input.return_expression(ExpressionType::Projection), None);

        Ok(())
    }

    #[test]
    fn first_item() -> anyhow::Result<()> {
        let mut input = expression::AliasList::default();

        assert_eq!(
            input.alias_value(AttributeValue::default())?,
            ":0".to_owned()
        );

        Ok(())
    }

    #[test]
    fn fifth_item() -> anyhow::Result<()> {
        let mut input = expression::AliasList {
            values: vec![
                AttributeValue::default(),
                AttributeValue::default(),
                AttributeValue::default(),
                AttributeValue::default(),
            ],
            ..Default::default()
        };

        assert_eq!(
            input.alias_value(AttributeValue::default())?,
            ":4".to_owned()
        );

        Ok(())
    }

    #[test]
    fn new_unique_item() -> anyhow::Result<()> {
        let mut input = expression::AliasList::default();

        assert_eq!(input.alias_path("foo")?, "#0".to_owned());

        Ok(())
    }

    #[test]
    fn duplicate_item() -> anyhow::Result<()> {
        let mut input = expression::AliasList {
            names: vec!["foo".to_owned(), "bar".to_owned()],
            ..Default::default()
        };

        assert_eq!(input.alias_path("foo")?, "#0".to_owned());

        Ok(())
    }
}
