use crate::{NameBuilder, OperandBuilder, SizeBuilder, ValueBuilder};

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

pub const DynamoDBAttributeTypeString: &str = "S";
pub const DynamoDBAttributeTypeStringSet: &str = "SS";
pub const DynamoDBAttributeTypeNumber: &str = "N";
pub const DynamoDBAttributeTypeNumberSet: &str = "NS";
pub const DynamoDBAttributeTypeBinary: &str = "B";
pub const DynamoDBAttributeTypeBinarySet: &str = "BS";
pub const DynamoDBAttributeTypeBoolean: &str = "BOOL";
pub const DynamoDBAttributeTypeNull: &str = "NULL";
pub const DynamoDBAttributeTypeList: &str = "L";
pub const DynamoDBAttributeTypeMap: &str = "M";

pub struct ConditionBuilder {
    operand_list: Option<Vec<Box<dyn OperandBuilder>>>,
    condition_list: Option<Vec<ConditionBuilder>>,
    mode: ConditionMode,
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

impl EqualBuilder for NameBuilder {}
impl NotEqualBuilder for NameBuilder {}
impl LessThanBuilder for NameBuilder {}
impl LessThanEqualBuilder for NameBuilder {}
impl GreaterThanBuilder for NameBuilder {}
impl GreaterThanEqualBuilder for NameBuilder {}

impl EqualBuilder for ValueBuilder {}
impl NotEqualBuilder for ValueBuilder {}
impl LessThanBuilder for ValueBuilder {}
impl LessThanEqualBuilder for ValueBuilder {}
impl GreaterThanBuilder for ValueBuilder {}
impl GreaterThanEqualBuilder for ValueBuilder {}

impl EqualBuilder for SizeBuilder {}
impl NotEqualBuilder for SizeBuilder {}
impl LessThanBuilder for SizeBuilder {}
impl LessThanEqualBuilder for SizeBuilder {}
impl GreaterThanBuilder for SizeBuilder {}
impl GreaterThanEqualBuilder for SizeBuilder {}
