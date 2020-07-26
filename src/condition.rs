use crate::{string_value, NameBuilder, OperandBuilder, SizeBuilder, ValueBuilder};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/condition.go

enum ConditionMode {
    //Unset,
    Equal,
    NotEqual,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
    And,
    Or,
    Not,
    Between,
    In,
    AttrExists,
    AttrNotExists,
    AttrType,
    BeginsWith,
    Contains,
}

trait EqualBuilder: OperandBuilder {
    fn equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        equal(self, right)
    }
}

trait NotEqualBuilder: OperandBuilder {
    fn not_equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        not_equal(self, right)
    }
}

trait LessThanBuilder: OperandBuilder {
    fn less_than(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        less_than(self, right)
    }
}

trait LessThanEqualBuilder: OperandBuilder {
    fn less_than_equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        less_than_equal(self, right)
    }
}

trait GreaterThanBuilder: OperandBuilder {
    fn greater_than(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        greater_than(self, right)
    }
}

trait GreaterThanEqualBuilder: OperandBuilder {
    fn greater_than_equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        greater_than_equal(self, right)
    }
}

trait BetweenBuilder: OperandBuilder {
    fn between(
        self: Box<Self>,
        upper: Box<dyn OperandBuilder>,
        lower: Box<dyn OperandBuilder>,
    ) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        between(self, upper, lower)
    }
}

trait InBuilder: OperandBuilder {
    fn r#in(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        r#in(self, right)
    }
}

#[derive(Copy, Clone)]
pub enum DynamoDBAttributeType {
    String,
    StringSet,
    Number,
    NumberSet,
    Binary,
    BinarySet,
    Boolean,
    Null,
    List,
    Map,
}

impl DynamoDBAttributeType {
    pub fn to_string(&self) -> &str {
        match self {
            DynamoDBAttributeType::String => "S",
            DynamoDBAttributeType::StringSet => "SS",
            DynamoDBAttributeType::Number => "N",
            DynamoDBAttributeType::NumberSet => "NS",
            DynamoDBAttributeType::Binary => "B",
            DynamoDBAttributeType::BinarySet => "BS",
            DynamoDBAttributeType::Boolean => "BOOL",
            DynamoDBAttributeType::Null => "NULL",
            DynamoDBAttributeType::List => "L",
            DynamoDBAttributeType::Map => "M",
        }
    }
}

pub struct ConditionBuilder {
    operand_list: Option<Vec<Box<dyn OperandBuilder>>>,
    condition_list: Option<Vec<ConditionBuilder>>,
    mode: ConditionMode,
}

impl ConditionBuilder {
    fn and(self, right: ConditionBuilder) -> ConditionBuilder {
        and(self, right)
    }

    fn or(self, right: ConditionBuilder) -> ConditionBuilder {
        or(self, right)
    }

    fn not(self) -> ConditionBuilder {
        not(self)
    }
}

pub fn equal(left: Box<dyn OperandBuilder>, right: Box<dyn OperandBuilder>) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Some(vec![left, right]),
        condition_list: None,
        mode: ConditionMode::Equal,
    }
}

pub fn not_equal(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Some(vec![left, right]),
        condition_list: None,
        mode: ConditionMode::NotEqual,
    }
}

pub fn less_than(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Some(vec![left, right]),
        condition_list: None,
        mode: ConditionMode::LessThan,
    }
}

pub fn less_than_equal(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Some(vec![left, right]),
        condition_list: None,
        mode: ConditionMode::LessThanEqual,
    }
}

pub fn greater_than(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Some(vec![left, right]),
        condition_list: None,
        mode: ConditionMode::GreaterThan,
    }
}

pub fn greater_than_equal(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Some(vec![left, right]),
        condition_list: None,
        mode: ConditionMode::GreaterThanEqual,
    }
}

pub fn and(left: ConditionBuilder, right: ConditionBuilder) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: None,
        condition_list: Some(vec![left, right]),
        mode: ConditionMode::And,
    }
}

pub fn or(left: ConditionBuilder, right: ConditionBuilder) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: None,
        condition_list: Some(vec![left, right]),
        mode: ConditionMode::Or,
    }
}

pub fn not(condition_builder: ConditionBuilder) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: None,
        condition_list: Some(vec![condition_builder]),
        mode: ConditionMode::Not,
    }
}

pub fn between(
    op: Box<dyn OperandBuilder>,
    lower: Box<dyn OperandBuilder>,
    upper: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Some(vec![op, lower, upper]),
        condition_list: None,
        mode: ConditionMode::Between,
    }
}

pub fn r#in(left: Box<dyn OperandBuilder>, right: Box<dyn OperandBuilder>) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Some(vec![left, right]),
        condition_list: None,
        mode: ConditionMode::In,
    }
}

pub fn attribute_exists(name: Box<NameBuilder>) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Some(vec![name]),
        condition_list: None,
        mode: ConditionMode::AttrExists,
    }
}

pub fn attribute_not_exists(name: Box<NameBuilder>) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Some(vec![name]),
        condition_list: None,
        mode: ConditionMode::AttrNotExists,
    }
}

pub fn attribute_type(
    name: Box<NameBuilder>,
    attr_type: DynamoDBAttributeType,
) -> ConditionBuilder {
    let v = string_value(attr_type.to_string());
    ConditionBuilder {
        operand_list: Some(vec![name, v]),
        condition_list: None,
        mode: ConditionMode::AttrType,
    }
}

pub fn begins_with(name: Box<NameBuilder>, prefix: impl Into<String>) -> ConditionBuilder {
    let v = string_value(prefix.into());
    ConditionBuilder {
        operand_list: Some(vec![name, v]),
        condition_list: None,
        mode: ConditionMode::BeginsWith,
    }
}

pub fn contains(name: Box<NameBuilder>, substr: impl Into<String>) -> ConditionBuilder {
    let v = string_value(substr.into());
    ConditionBuilder {
        operand_list: Some(vec![name, v]),
        condition_list: None,
        mode: ConditionMode::Contains,
    }
}

impl NameBuilder {
    pub fn attribute_exists(self: Box<NameBuilder>) -> ConditionBuilder {
        attribute_exists(self)
    }

    pub fn attribute_not_exists(self: Box<NameBuilder>) -> ConditionBuilder {
        attribute_not_exists(self)
    }

    pub fn attribute_type(
        self: Box<NameBuilder>,
        attr_type: DynamoDBAttributeType,
    ) -> ConditionBuilder {
        attribute_type(self, attr_type)
    }

    pub fn begins_with(self: Box<NameBuilder>, prefix: impl Into<String>) -> ConditionBuilder {
        begins_with(self, prefix)
    }

    pub fn contains(self: Box<NameBuilder>, substr: impl Into<String>) -> ConditionBuilder {
        contains(self, substr)
    }
}

impl EqualBuilder for NameBuilder {}
impl NotEqualBuilder for NameBuilder {}
impl LessThanBuilder for NameBuilder {}
impl LessThanEqualBuilder for NameBuilder {}
impl GreaterThanBuilder for NameBuilder {}
impl GreaterThanEqualBuilder for NameBuilder {}
impl BetweenBuilder for NameBuilder {}
impl InBuilder for NameBuilder {}

impl EqualBuilder for ValueBuilder {}
impl NotEqualBuilder for ValueBuilder {}
impl LessThanBuilder for ValueBuilder {}
impl LessThanEqualBuilder for ValueBuilder {}
impl GreaterThanBuilder for ValueBuilder {}
impl GreaterThanEqualBuilder for ValueBuilder {}
impl BetweenBuilder for ValueBuilder {}
impl InBuilder for ValueBuilder {}

impl EqualBuilder for SizeBuilder {}
impl NotEqualBuilder for SizeBuilder {}
impl LessThanBuilder for SizeBuilder {}
impl LessThanEqualBuilder for SizeBuilder {}
impl GreaterThanBuilder for SizeBuilder {}
impl GreaterThanEqualBuilder for SizeBuilder {}
impl BetweenBuilder for SizeBuilder {}
impl InBuilder for SizeBuilder {}
