use crate::{string_value, ExpressionNode, KeyBuilder, OperandBuilder, TreeBuilder, ValueBuilder};

// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/key_condition.go

#[derive(Copy, Clone, PartialEq, Debug)]
enum KeyConditionMode {
    //Unset,
    Invalid,
    Equal,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
    And,
    Between,
    BeginsWith,
}

pub struct KeyConditionBuilder {
    operand_list: Option<Vec<Box<dyn OperandBuilder>>>,
    key_condition_list: Option<Vec<KeyConditionBuilder>>,
    mode: KeyConditionMode,
}

impl KeyConditionBuilder {
    pub fn and(self, right: KeyConditionBuilder) -> KeyConditionBuilder {
        key_and(self, right)
    }
}

impl TreeBuilder for KeyConditionBuilder {
    fn build_tree(&self) -> anyhow::Result<ExpressionNode> {
        unimplemented!("KeyConditionBuilder::build_tree")
    }
}

pub fn key_equal(key: Box<KeyBuilder>, value: Box<ValueBuilder>) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: Some(vec![key, value]),
        key_condition_list: None,
        mode: KeyConditionMode::Equal,
    }
}

pub fn key_less_than(key: Box<KeyBuilder>, value: Box<ValueBuilder>) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: Some(vec![key, value]),
        key_condition_list: None,
        mode: KeyConditionMode::LessThan,
    }
}

pub fn key_less_than_equal(key: Box<KeyBuilder>, value: Box<ValueBuilder>) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: Some(vec![key, value]),
        key_condition_list: None,
        mode: KeyConditionMode::LessThanEqual,
    }
}

pub fn key_greater_than(key: Box<KeyBuilder>, value: Box<ValueBuilder>) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: Some(vec![key, value]),
        key_condition_list: None,
        mode: KeyConditionMode::GreaterThan,
    }
}

pub fn key_greater_than_equal(
    key: Box<KeyBuilder>,
    value: Box<ValueBuilder>,
) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: Some(vec![key, value]),
        key_condition_list: None,
        mode: KeyConditionMode::GreaterThanEqual,
    }
}

pub fn key_and(left: KeyConditionBuilder, right: KeyConditionBuilder) -> KeyConditionBuilder {
    if left.mode != KeyConditionMode::Equal {
        return KeyConditionBuilder {
            operand_list: None,
            key_condition_list: None,
            mode: KeyConditionMode::Invalid,
        };
    }

    if right.mode == KeyConditionMode::And {
        return KeyConditionBuilder {
            operand_list: None,
            key_condition_list: None,
            mode: KeyConditionMode::Invalid,
        };
    }

    KeyConditionBuilder {
        operand_list: None,
        key_condition_list: Some(vec![left, right]),
        mode: KeyConditionMode::And,
    }
}

pub fn key_between(
    key: Box<KeyBuilder>,
    upper: Box<ValueBuilder>,
    lower: Box<ValueBuilder>,
) -> KeyConditionBuilder {
    KeyConditionBuilder {
        operand_list: Some(vec![key, upper, lower]),
        key_condition_list: None,
        mode: KeyConditionMode::Between,
    }
}

pub fn key_begins_with(key: Box<KeyBuilder>, prefix: impl Into<String>) -> KeyConditionBuilder {
    let v = string_value(prefix.into());
    KeyConditionBuilder {
        operand_list: Some(vec![key, v]),
        key_condition_list: None,
        mode: KeyConditionMode::BeginsWith,
    }
}

impl KeyBuilder {
    pub fn qual(self: Box<KeyBuilder>, value: Box<ValueBuilder>) -> KeyConditionBuilder {
        key_equal(self, value)
    }

    pub fn less_than(self: Box<KeyBuilder>, value: Box<ValueBuilder>) -> KeyConditionBuilder {
        key_less_than(self, value)
    }

    pub fn less_than_equal(self: Box<KeyBuilder>, value: Box<ValueBuilder>) -> KeyConditionBuilder {
        key_less_than_equal(self, value)
    }

    pub fn greater_than(self: Box<KeyBuilder>, value: Box<ValueBuilder>) -> KeyConditionBuilder {
        key_greater_than(self, value)
    }

    pub fn greater_than_equal(
        self: Box<KeyBuilder>,
        value: Box<ValueBuilder>,
    ) -> KeyConditionBuilder {
        key_greater_than_equal(self, value)
    }

    pub fn between(
        self: Box<KeyBuilder>,
        upper: Box<ValueBuilder>,
        lower: Box<ValueBuilder>,
    ) -> KeyConditionBuilder {
        key_between(self, upper, lower)
    }

    pub fn begins_with(self: Box<KeyBuilder>, prefix: impl Into<String>) -> KeyConditionBuilder {
        key_begins_with(self, prefix)
    }
}
