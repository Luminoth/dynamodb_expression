use std::collections::HashMap;

use rusoto_dynamodb::AttributeValue;

use crate::{ConditionBuilder, KeyConditionBuilder, ProjectionBuilder, UpdateBuilder};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/expression.go

#[derive(Hash, Eq, PartialEq, Debug)]
enum ExpressionType {
    Projection,
    KeyCondition,
    Condition,
    Filter,
    Update,
}

pub struct Expression {
    expressions: HashMap<ExpressionType, String>,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
}

impl Expression {
    pub fn condition(&self) -> String {
        unimplemented!("Expression::condition")
    }

    pub fn filter(&self) -> String {
        unimplemented!("Expression::filter")
    }

    pub fn projection(&self) -> String {
        unimplemented!("Expression::projection")
    }

    pub fn key_condition(&self) -> String {
        unimplemented!("Expression::key_condition")
    }

    pub fn update(&self) -> String {
        unimplemented!("Expression::update")
    }

    pub fn names(&self) -> &HashMap<String, String> {
        &self.names
    }

    pub fn values(&self) -> &HashMap<String, AttributeValue> {
        &self.values
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

    pub fn build() -> anyhow::Result<Expression> {
        unimplemented!("Builder::build")
    }
}

pub(crate) trait TreeBuilder {
    fn build_tree(&self) -> anyhow::Result<ExpressionNode>;
}

pub(crate) struct ExpressionNode {
    names: Vec<String>,
    values: Vec<AttributeValue>,
    children: Vec<ExpressionNode>,
    fmt_expression: String,
}
