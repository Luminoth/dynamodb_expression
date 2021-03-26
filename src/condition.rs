//! Ported from [condition.go](https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/condition.go)

use anyhow::bail;
use derivative::*;

use crate::{
    error::ExpressionError, value, ExpressionNode, NameBuilder, OperandBuilder, SizeBuilder,
    TreeBuilder,
};

/// Specifies the types of the struct conditionBuilder,
/// representing the different types of Conditions (i.e. And, Or, Between, ...)
#[derive(Copy, Clone, PartialEq, Debug, Derivative)]
#[derivative(Default)]
enum ConditionMode {
    /// Unset catches errors for unset ConditionBuilder structs
    #[derivative(Default)]
    Unset,

    /// Equal represents the Equals Condition
    Equal,

    /// NotEqual represents the Not Equals Condition
    NotEqual,

    /// LessThan represents the LessThan Condition
    LessThan,

    /// LessThanEqual represents the LessThanOrEqual Condition
    LessThanEqual,

    /// GreaterThan represents the GreaterThan Condition
    GreaterThan,

    /// GreaterThanEqual represents the GreaterThanEqual Condition
    GreaterThanEqual,

    /// And represents the Logical And Condition
    And,

    /// Or represents the Logical Or Condition
    Or,

    /// Not represents the Logical Not Condition
    Not,

    /// Between represents the Between Condition
    Between,

    /// In represents the In Condition
    In,

    /// AttrExists represents the Attribute Exists Condition
    AttrExists,

    /// AttrNotExists represents the Attribute Not Exists Condition
    AttrNotExists,

    /// AttrType represents the Attribute Type Condition
    AttrType,

    /// BeginsWith represents the Begins With Condition
    BeginsWith,

    // Contains represents the Contains Condition
    Contains,
}

/// Specifies the type of an DynamoDB item attribute.
///
/// This enum is used in the AttributeType() function in order to be explicit about
/// the DynamoDB type that is being checked and ensure compile time checks.
///
/// [More Information](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Functions)
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum DynamoDbAttributeType {
    /// String represents the DynamoDB String type
    String,

    /// StringSet represents the DynamoDB String Set type
    StringSet,

    /// Number represents the DynamoDB Number type
    Number,

    /// NumberSet represents the DynamoDB Number Set type
    NumberSet,

    /// Binary represents the DynamoDB Binary type
    Binary,

    /// BinarySet represents the DynamoDB Binary Set type
    BinarySet,

    /// Boolean represents the DynamoDB Boolean type
    Boolean,

    /// Null represents the DynamoDB Null type
    Null,

    /// List represents the DynamoDB List type
    List,

    /// Map represents the DynamoDB Map type
    Map,
}

impl DynamoDbAttributeType {
    /// Returns the string representation of the DynamoDbAttributeType
    pub fn as_str(&self) -> &str {
        match self {
            DynamoDbAttributeType::String => "S",
            DynamoDbAttributeType::StringSet => "SS",
            DynamoDbAttributeType::Number => "N",
            DynamoDbAttributeType::NumberSet => "NS",
            DynamoDbAttributeType::Binary => "B",
            DynamoDbAttributeType::BinarySet => "BS",
            DynamoDbAttributeType::Boolean => "BOOL",
            DynamoDbAttributeType::Null => "NULL",
            DynamoDbAttributeType::List => "L",
            DynamoDbAttributeType::Map => "M",
        }
    }
}

/// Represents Condition Expressions and Filter Expressions in DynamoDB.
///
/// ConditionBuilders are one of the building blocks of the Builder struct.
/// Since Filter Expressions support all the same functions and formats
/// as Condition Expressions, ConditionBuilders represents both types of Expressions.
///
/// [More Information](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)
///
/// [More Information on Filter Expressions](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.FilterExpression)
#[derive(Default)]
pub struct ConditionBuilder {
    operand_list: Vec<Box<dyn OperandBuilder>>,
    condition_list: Vec<ConditionBuilder>,
    mode: ConditionMode,
}

impl ConditionBuilder {
    /// Returns a ConditionBuilder representing the logical AND clause of the argument ConditionBuilders.
    ///
    /// The resulting ConditionBuilder can be used as a
    /// part of other Condition Expressions or as an argument to the with_condition()
    /// method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the condition where the item attribute "Name" is
    /// // equal to value "Generic Name" AND the item attribute "Age" is less
    /// // than value 40
    /// let condition = name("Name").equal(value("Generic Name")).and(name("Age").less_than(value(40)));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    // TODO: variadic
    pub fn and(self, right: ConditionBuilder) -> ConditionBuilder {
        and(self, right)
    }

    /// Returns a ConditionBuilder representing the logical OR clause of the argument ConditionBuilders.
    ///
    /// The resulting ConditionBuilder can be used as a
    /// part of other Condition Expressions or as an argument to the with_condition()
    /// method for the Builder struct. Note that Or() can take a variadic number of
    /// ConditionBuilders as arguments.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the condition where the item attribute "Price" is
    /// // less than the value 100 OR the item attribute "Rating" is greater than
    /// // the value 8
    /// let condition = name("Price").equal(value(100)).or(name("Rating").less_than(value(8)));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    // TODO: variadic
    pub fn or(self, right: ConditionBuilder) -> ConditionBuilder {
        or(self, right)
    }

    /// Returns a ConditionBuilder representing the logical NOT clause of the argument ConditionBuilder.
    ///
    /// The resulting ConditionBuilder can be used as a
    /// part of other Condition Expressions or as an argument to the with_condition()
    /// method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the condition where the item attribute "Name"
    /// // does not begin with "test"
    /// let condition = name("Name").begins_with("test").not();
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> ConditionBuilder {
        not(self)
    }

    fn build_child_nodes(&self) -> anyhow::Result<Vec<ExpressionNode>> {
        let mut child_nodes = Vec::new();

        for condition in self.condition_list.iter() {
            let node = condition.build_tree()?;
            child_nodes.push(node);
        }

        for ope in self.operand_list.iter() {
            let operand = ope.build_operand()?;
            child_nodes.push(operand.expression_node);
        }

        Ok(child_nodes)
    }

    fn compare_build_condition(
        mode: ConditionMode,
        mut node: ExpressionNode,
    ) -> anyhow::Result<ExpressionNode> {
        match mode {
            ConditionMode::Equal => node.fmt_expression = "$c = $c".to_owned(),
            ConditionMode::NotEqual => node.fmt_expression = "$c <> $c".to_owned(),
            ConditionMode::LessThan => node.fmt_expression = "$c < $c".to_owned(),
            ConditionMode::LessThanEqual => node.fmt_expression = "$c <= $c".to_owned(),
            ConditionMode::GreaterThan => node.fmt_expression = "$c > $c".to_owned(),
            ConditionMode::GreaterThanEqual => node.fmt_expression = "$c >= $c".to_owned(),
            _ => bail!(
                "build compare condition error: unsupported mode: {:?}",
                mode
            ),
        }
        Ok(node)
    }

    fn compound_build_condition(
        condition_builder: &ConditionBuilder,
        mut node: ExpressionNode,
    ) -> anyhow::Result<ExpressionNode> {
        // create a string with escaped characters to substitute them with proper
        // aliases during runtime
        let mode = match condition_builder.mode {
            ConditionMode::And => " AND ",
            ConditionMode::Or => " OR ",
            _ => bail!(
                "build compound condition error: unsupported mode: {:?}",
                condition_builder.mode
            ),
        };

        node.fmt_expression = format!(
            "($c){}",
            format!("{}{}", mode, "($c)").repeat(condition_builder.condition_list.len() - 1)
        );

        Ok(node)
    }

    fn not_build_condition(mut node: ExpressionNode) -> ExpressionNode {
        // create a string with escaped characters to substitute them with proper
        // aliases during runtime
        node.fmt_expression = "NOT ($c)".to_owned();

        node
    }

    fn between_build_condition(mut node: ExpressionNode) -> ExpressionNode {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "$c BETWEEN $c AND $c".to_owned();

        node
    }

    fn in_build_condition(
        condition_builder: &ConditionBuilder,
        mut node: ExpressionNode,
    ) -> ExpressionNode {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = format!(
            "$c IN ($c{})",
            ", $c".repeat(condition_builder.operand_list.len() - 2)
        );

        node
    }

    fn attr_exists_build_condition(mut node: ExpressionNode) -> ExpressionNode {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "attribute_exists ($c)".to_owned();

        node
    }

    fn attr_not_exists_build_condition(mut node: ExpressionNode) -> ExpressionNode {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "attribute_not_exists ($c)".to_owned();

        node
    }

    fn attr_type_build_condition(mut node: ExpressionNode) -> ExpressionNode {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "attribute_type ($c, $c)".to_owned();

        node
    }

    fn begins_with_build_condition(mut node: ExpressionNode) -> ExpressionNode {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "begins_with ($c, $c)".to_owned();

        node
    }

    fn contains_build_condition(mut node: ExpressionNode) -> ExpressionNode {
        // Create a string with special characters that can be substituted later: $c
        node.fmt_expression = "contains ($c, $c)".to_owned();

        node
    }
}

impl TreeBuilder for ConditionBuilder {
    fn build_tree(&self) -> anyhow::Result<ExpressionNode> {
        let child_nodes = self.build_child_nodes()?;
        let ret = ExpressionNode::from_children(child_nodes);

        match self.mode {
            ConditionMode::Equal
            | ConditionMode::NotEqual
            | ConditionMode::LessThan
            | ConditionMode::LessThanEqual
            | ConditionMode::GreaterThan
            | ConditionMode::GreaterThanEqual => {
                Ok(ConditionBuilder::compare_build_condition(self.mode, ret)?)
            }
            ConditionMode::And | ConditionMode::Or => {
                Ok(ConditionBuilder::compound_build_condition(self, ret)?)
            }
            ConditionMode::Not => Ok(ConditionBuilder::not_build_condition(ret)),
            ConditionMode::Between => Ok(ConditionBuilder::between_build_condition(ret)),
            ConditionMode::In => Ok(ConditionBuilder::in_build_condition(self, ret)),
            ConditionMode::AttrExists => Ok(ConditionBuilder::attr_exists_build_condition(ret)),
            ConditionMode::AttrNotExists => {
                Ok(ConditionBuilder::attr_not_exists_build_condition(ret))
            }
            ConditionMode::AttrType => Ok(ConditionBuilder::attr_type_build_condition(ret)),
            ConditionMode::BeginsWith => Ok(ConditionBuilder::begins_with_build_condition(ret)),
            ConditionMode::Contains => Ok(ConditionBuilder::contains_build_condition(ret)),
            ConditionMode::Unset => bail!(ExpressionError::UnsetParameterError(
                "buildTree".to_owned(),
                "ConditionBuilder".to_owned(),
            )),
        }
    }
}

/// Returns a ConditionBuilder representing the equality clause of the two argument OperandBuilders.
///
/// The resulting ConditionBuilder can be used as a
/// part of other Condition Expressions or as an argument to the with_condition()
/// method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the equal clause of the item attribute "foo" and
/// // the value 5
/// let condition = equal(name("foo"), value(5));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn equal(left: Box<dyn OperandBuilder>, right: Box<dyn OperandBuilder>) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::Equal,
    }
}

/// Returns a ConditionBuilder representing the not equal clause of the two argument OperandBuilders.
///
/// The resulting ConditionBuilder can be used as a
/// part of other Condition Expressions or as an argument to the with_condition()
/// method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the not equal clause of the item attribute "foo"
/// // and the value 5
/// let condition = not_equal(name("foo"), value(5));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn not_equal(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::NotEqual,
    }
}

/// Returns a ConditionBuilder representing the less than clause of the two argument OperandBuilders.
///
/// The resulting ConditionBuilder can be used as a
/// part of other Condition Expressions or as an argument to the with_condition()
/// method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the less than clause of the item attribute "foo"
/// // and the value 5
/// let condition = less_than(name("foo"), value(5));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn less_than(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::LessThan,
    }
}

/// Returns a ConditionBuilder representing the less than equal to clause of the two argument OperandBuilders.
///
/// The resulting ConditionBuilder can be used as a
/// part of other Condition Expressions or as an argument to the with_condition()
/// method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the less than equal to clause of the item attribute "foo"
/// // and the value 5
/// let condition = less_than_equal(name("foo"), value(5));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn less_than_equal(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::LessThanEqual,
    }
}

/// Returns a ConditionBuilder representing the greater than clause of the two argument OperandBuilders.
///
/// The resulting ConditionBuilder can be used as a
/// part of other Condition Expressions or as an argument to the with_condition()
/// method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the greater than clause of the item attribute "foo"
/// // and the value 5
/// let condition = greater_than(name("foo"), value(5));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn greater_than(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::GreaterThan,
    }
}

/// Returns a ConditionBuilder representing the greater than equal to clause of the two argument OperandBuilders.
///
/// The resulting ConditionBuilder can be used as a
/// part of other Condition Expressions or as an argument to the with_condition()
/// method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the greater than equal to clause of the item attribute "foo"
/// // and the value 5
/// let condition = greater_than_equal(name("foo"), value(5));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn greater_than_equal(
    left: Box<dyn OperandBuilder>,
    right: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![left, right],
        condition_list: Vec::new(),
        mode: ConditionMode::GreaterThanEqual,
    }
}

/// Returns a ConditionBuilder representing the logical AND clause of the argument ConditionBuilders.
///
/// The resulting ConditionBuilder can be used as a
/// part of other Condition Expressions or as an argument to the with_condition()
/// method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the condition where the item attribute "Name" is
/// // equal to value "Generic Name" AND the item attribute "Age" is less
/// // than value 40
/// let condition = and(name("Name").equal(value("Generic Name")), name("Age").less_than(value(40)));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
// TODO: variadic
pub fn and(left: ConditionBuilder, right: ConditionBuilder) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Vec::new(),
        condition_list: vec![left, right],
        mode: ConditionMode::And,
    }
}

/// Returns a ConditionBuilder representing the logical OR clause of the argument ConditionBuilders.
///
/// The resulting ConditionBuilder can be used as a
/// part of other Condition Expressions or as an argument to the with_condition()
/// method for the Builder struct. Note that Or() can take a variadic number of
/// ConditionBuilders as arguments.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the condition where the item attribute "Price" is
/// // less than the value 100 OR the item attribute "Rating" is greater than
/// // the value 8
/// let condition = or(name("Price").equal(value(100)), name("Rating").less_than(value(8)));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
// TODO: variadic
pub fn or(left: ConditionBuilder, right: ConditionBuilder) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Vec::new(),
        condition_list: vec![left, right],
        mode: ConditionMode::Or,
    }
}

/// Returns a ConditionBuilder representing the logical NOT clause of the argument ConditionBuilder.
///
/// The resulting ConditionBuilder can be used as a
/// part of other Condition Expressions or as an argument to the with_condition()
/// method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the condition where the item attribute "Name"
/// // does not begin with "test"
/// let condition = not(name("Name").begins_with("test"));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn not(condition_builder: ConditionBuilder) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: Vec::new(),
        condition_list: vec![condition_builder],
        mode: ConditionMode::Not,
    }
}

/// Returns a ConditionBuilder representing the result of the
/// BETWEEN function in DynamoDB Condition Expressions.
///
/// The resulting ConditionBuilder can be used as a part of other Condition Expressions or as
/// an argument to the with_condition() method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the condition where the value of the item
/// // attribute "Rating" is between values 5 and 10
/// let condition = between(name("Rating"), value(5), value(10));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn between(
    op: Box<dyn OperandBuilder>,
    lower: Box<dyn OperandBuilder>,
    upper: Box<dyn OperandBuilder>,
) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![op, lower, upper],
        condition_list: Vec::new(),
        mode: ConditionMode::Between,
    }
}

/// Returns a ConditionBuilder representing the result of the IN function
/// in DynamoDB Condition Expressions.
///
/// The resulting ConditionBuilder can be used
/// as a part of other Condition Expressions or as an argument to the
/// with_condition() method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the condition where the value of the item
/// // attribute "Color" is checked against the list of colors "red",
/// // "green", and "blue".
/// let condition = r#in(name("Color"), vec![value("red"), value("green"), value("blue")]);
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn r#in(
    left: Box<dyn OperandBuilder>,
    mut right: Vec<Box<dyn OperandBuilder>>,
) -> ConditionBuilder {
    let mut operand_list = vec![left];
    operand_list.append(&mut right);

    ConditionBuilder {
        operand_list,
        condition_list: Vec::new(),
        mode: ConditionMode::In,
    }
}

/// Returns a ConditionBuilder representing the result of the
/// attribute_exists function in DynamoDB Condition Expressions.
///
/// The resulting ConditionBuilder can be used as a part of other Condition Expressions or as
/// an argument to the with_condition() method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the boolean condition of whether the item
/// // attribute "Age" exists or not
/// let condition = attribute_exists(name("Age"));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn attribute_exists(name: Box<NameBuilder>) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![name],
        condition_list: Vec::new(),
        mode: ConditionMode::AttrExists,
    }
}

/// Returns a ConditionBuilder representing the result of
/// the attribute_not_exists function in DynamoDB Condition Expressions.
///
/// The resulting ConditionBuilder can be used as a part of other Condition
/// Expressions or as an argument to the with_condition() method for the Builder
/// struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the boolean condition of whether the item
/// // attribute "Age" exists or not
/// let condition = attribute_not_exists(name("Age"));
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn attribute_not_exists(name: Box<NameBuilder>) -> ConditionBuilder {
    ConditionBuilder {
        operand_list: vec![name],
        condition_list: Vec::new(),
        mode: ConditionMode::AttrNotExists,
    }
}

/// Returns a ConditionBuilder representing the result of the
/// attribute_type function in DynamoDB Condition Expressions.
///
/// The DynamoDB types are represented by the type DynamoDbAttributeType. The resulting
/// ConditionBuilder can be used as a part of other Condition Expressions or as
/// an argument to the with_condition() method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the boolean condition of whether the item
/// // attribute "Age" has the DynamoDB type Number or not
/// let condition = attribute_type(name("Age"), DynamoDbAttributeType::Number);
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn attribute_type(
    name: Box<NameBuilder>,
    attr_type: DynamoDbAttributeType,
) -> ConditionBuilder {
    let v = value(attr_type.as_str().to_owned());
    ConditionBuilder {
        operand_list: vec![name, v],
        condition_list: Vec::new(),
        mode: ConditionMode::AttrType,
    }
}

/// BeginsWith returns a ConditionBuilder representing the result of the
/// begins_with function in DynamoDB Condition Expressions.
///
/// The resulting ConditionBuilder can be used as a part of other Condition Expressions or as
/// an argument to the WithCondition() method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the boolean condition of whether the item
/// // attribute "CodeName" starts with the substring "Ben"
/// let condition = begins_with(name("CodeName"), "Ben");
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn begins_with(name: Box<NameBuilder>, prefix: impl Into<String>) -> ConditionBuilder {
    let v = value(prefix.into());
    ConditionBuilder {
        operand_list: vec![name, v],
        condition_list: Vec::new(),
        mode: ConditionMode::BeginsWith,
    }
}

/// Returns a ConditionBuilder representing the result of the
/// contains function in DynamoDB Condition Expressions.
///
/// The resulting ConditionBuilder can be used as a part of other Condition Expressions or as
/// an argument to the WithCondition() method for the Builder struct.
///
/// # Example
///
/// ```
/// use dynamodb_expression::*;
///
/// // condition represents the boolean condition of whether the item
/// // attribute "InviteList" has the value "Ben"
/// let condition = contains(name("InviteList"), "Ben");
///
/// // Used in another Condition Expression
/// let another_condition = not(condition);
/// // Used to make an Builder
/// let builder = Builder::new().with_condition(another_condition);
/// ```
pub fn contains(name: Box<NameBuilder>, substr: impl Into<String>) -> ConditionBuilder {
    let v = value(substr.into());
    ConditionBuilder {
        operand_list: vec![name, v],
        condition_list: Vec::new(),
        mode: ConditionMode::Contains,
    }
}

pub trait EqualBuilder: OperandBuilder {
    /// Returns a ConditionBuilder representing the equality clause of the two argument OperandBuilders.
    ///
    /// The resulting ConditionBuilder can be used as a
    /// part of other Condition Expressions or as an argument to the with_condition()
    /// method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the equal clause of the item attribute "foo" and
    /// // the value 5
    /// let condition = equal(name("foo"), value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the equal clause of the item attribute "foo" and
    /// // the value 5
    /// let condition = value(5).equal(name("foo"));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the equal clause of the size of the item
    /// // attribute "foo" and the value 5
    /// let condition = size(name("foo")).equal(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    fn equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        equal(self, right)
    }
}

pub trait NotEqualBuilder: OperandBuilder {
    /// Returns a ConditionBuilder representing the not equal clause of the two argument OperandBuilders.
    ///
    /// The resulting ConditionBuilder can be used as a
    /// part of other Condition Expressions or as an argument to the with_condition()
    /// method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the not equal clause of the item attribute "foo"
    /// // and the value 5
    /// let condition = name("foo").not_equal(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the not equal clause of the item attribute "foo"
    /// // and the value 5
    /// let condition = value(5).not_equal(name("foo"));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// Example:
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the not equal clause of the size of the item
    /// // attribute "foo" and the value 5
    /// let condition = size(name("foo")).not_equal(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    fn not_equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        not_equal(self, right)
    }
}

pub trait LessThanBuilder: OperandBuilder {
    /// Returns a ConditionBuilder representing the less than clause of the two argument OperandBuilders.
    ///
    /// The resulting ConditionBuilder can be used as a
    /// part of other Condition Expressions or as an argument to the with_condition()
    /// method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the less than clause of the item attribute "foo"
    /// // and the value 5
    /// let condition = name("foo").less_than(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the less than clause of the item attribute "foo"
    /// // and the value 5
    /// let condition = value(5).less_than(name("foo"));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the less than clause of the size of the item
    /// // attribute "foo" and the value 5
    /// let condition = size(name("foo")).less_than(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    fn less_than(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        less_than(self, right)
    }
}

pub trait LessThanEqualBuilder: OperandBuilder {
    /// Returns a ConditionBuilder representing the less than equal to clause of the two argument OperandBuilders.
    ///
    /// The resulting ConditionBuilder can be used as a
    /// part of other Condition Expressions or as an argument to the with_condition()
    /// method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the less than equal to clause of the item attribute "foo"
    /// // and the value 5
    /// let condition = name("foo").less_than_equal(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the less than equal to clause of the item attribute "foo"
    /// // and the value 5
    /// let condition = value(5).less_than_equal(name("foo"));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the less than equal to clause of the size of the item
    /// // attribute "foo" and the value 5
    /// let condition = size(name("foo")).less_than_equal(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    fn less_than_equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        less_than_equal(self, right)
    }
}

pub trait GreaterThanBuilder: OperandBuilder {
    /// Returns a ConditionBuilder representing the greater than clause of the two argument OperandBuilders.
    ///
    /// The resulting ConditionBuilder can be used as a
    /// part of other Condition Expressions or as an argument to the with_condition()
    /// method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the greater than clause of the item attribute "foo"
    /// // and the value 5
    /// let condition = name("foo").greater_than(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the greater than clause of the item attribute "foo"
    /// // and the value 5
    /// let condition = value(5).greater_than(name("foo"));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the greater than clause of the size of the item
    /// // attribute "foo" and the value 5
    /// let condition = size(name("foo")).greater_than(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    fn greater_than(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        greater_than(self, right)
    }
}

pub trait GreaterThanEqualBuilder: OperandBuilder {
    /// Returns a ConditionBuilder representing the greater than equal to clause of the two argument OperandBuilders.
    ///
    /// The resulting ConditionBuilder can be used as a
    /// part of other Condition Expressions or as an argument to the with_condition()
    /// method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the greater than equal to clause of the item attribute "foo"
    /// // and the value 5
    /// let condition = name("foo").greater_than_equal(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the greater than equal to clause of the item attribute "foo"
    /// // and the value 5
    /// let condition = value(5).greater_than_equal(name("foo"));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the greater than equal to clause of the size of the item
    /// // attribute "foo" and the value 5
    /// let condition = size(name("foo")).greater_than_equal(value(5));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    fn greater_than_equal(self: Box<Self>, right: Box<dyn OperandBuilder>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        greater_than_equal(self, right)
    }
}

pub trait BetweenBuilder: OperandBuilder {
    /// Returns a ConditionBuilder representing the result of the
    /// BETWEEN function in DynamoDB Condition Expressions.
    ///
    /// The resulting ConditionBuilder can be used as a part of other Condition Expressions or as
    /// an argument to the with_condition() method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the condition where the value of the item
    /// // attribute "Rating" is between values 5 and 10
    /// let condition = name("Rating").between(value(5), value(10));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the condition where the value of the item
    /// // attribute "Rating" is between values 5 and 10
    /// let condition = value(6).between(value(5), value(10));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the condition where the value of the item
    /// // attribute "Rating" is between values 5 and 10
    /// let condition = size(name("InviteList")).between(value(5), value(10));
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
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

pub trait InBuilder: OperandBuilder {
    /// Returns a ConditionBuilder representing the result of the IN function
    /// in DynamoDB Condition Expressions.
    ///
    /// The resulting ConditionBuilder can be used
    /// as a part of other Condition Expressions or as an argument to the
    /// with_condition() method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the condition where the value of the item
    /// // attribute "Color" is checked against the list of colors "red",
    /// // "green", and "blue".
    /// let condition = name("Color").r#in(vec![value("red"), value("green"), value("blue")]);
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the condition where the value of the item
    /// // attribute "Color" is checked against the list of colors "red",
    /// // "green", and "blue".
    /// let condition = value("yellow").r#in(vec![value("red"), value("green"), value("blue")]);
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the condition where the value of the item
    /// // attribute "Color" is checked against the list of colors "red",
    /// // "green", and "blue".
    /// let condition = size(name("Donuts")).r#in(vec![value(12), value(24), value(36)]);
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    fn r#in(self: Box<Self>, right: Vec<Box<dyn OperandBuilder>>) -> ConditionBuilder
    where
        Self: Sized + 'static,
    {
        r#in(self, right)
    }
}

impl NameBuilder {
    /// Returns a ConditionBuilder representing the result of the
    /// attribute_exists function in DynamoDB Condition Expressions.
    ///
    /// The resulting ConditionBuilder can be used as a part of other Condition Expressions or as
    /// an argument to the with_condition() method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the boolean condition of whether the item
    /// // attribute "Age" exists or not
    /// let condition = name("Age").attribute_exists();
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    pub fn attribute_exists(self: Box<NameBuilder>) -> ConditionBuilder {
        attribute_exists(self)
    }

    /// Returns a ConditionBuilder representing the result of
    /// the attribute_not_exists function in DynamoDB Condition Expressions.
    ///
    /// The resulting ConditionBuilder can be used as a part of other Condition
    /// Expressions or as an argument to the with_condition() method for the Builder
    /// struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the boolean condition of whether the item
    /// // attribute "Age" exists or not
    /// let condition = name("Age").attribute_not_exists();
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    pub fn attribute_not_exists(self: Box<NameBuilder>) -> ConditionBuilder {
        attribute_not_exists(self)
    }

    /// Returns a ConditionBuilder representing the result of the
    /// attribute_type function in DynamoDB Condition Expressions.
    ///
    /// The DynamoDB types are represented by the type DynamoDbAttributeType. The resulting
    /// ConditionBuilder can be used as a part of other Condition Expressions or as
    /// an argument to the with_condition() method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the boolean condition of whether the item
    /// // attribute "Age" has the DynamoDB type Number or not
    /// let condition = name("Age").attribute_type(DynamoDbAttributeType::Number);
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    pub fn attribute_type(
        self: Box<NameBuilder>,
        attr_type: DynamoDbAttributeType,
    ) -> ConditionBuilder {
        attribute_type(self, attr_type)
    }

    /// BeginsWith returns a ConditionBuilder representing the result of the
    /// begins_with function in DynamoDB Condition Expressions.
    ///
    /// The resulting ConditionBuilder can be used as a part of other Condition Expressions or as
    /// an argument to the WithCondition() method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the boolean condition of whether the item
    /// // attribute "CodeName" starts with the substring "Ben"
    /// let condition = name("CodeName").begins_with("Ben");
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
    pub fn begins_with(self: Box<NameBuilder>, prefix: impl Into<String>) -> ConditionBuilder {
        begins_with(self, prefix)
    }

    /// Returns a ConditionBuilder representing the result of the
    /// contains function in DynamoDB Condition Expressions.
    ///
    /// The resulting ConditionBuilder can be used as a part of other Condition Expressions or as
    /// an argument to the WithCondition() method for the Builder struct.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // condition represents the boolean condition of whether the item
    /// // attribute "InviteList" has the value "Ben"
    /// let condition = name("InviteList").contains("Ben");
    ///
    /// // Used in another Condition Expression
    /// let another_condition = not(condition);
    /// // Used to make an Builder
    /// let builder = Builder::new().with_condition(another_condition);
    /// ```
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

impl EqualBuilder for SizeBuilder {}
impl NotEqualBuilder for SizeBuilder {}
impl LessThanBuilder for SizeBuilder {}
impl LessThanEqualBuilder for SizeBuilder {}
impl GreaterThanBuilder for SizeBuilder {}
impl GreaterThanEqualBuilder for SizeBuilder {}
impl BetweenBuilder for SizeBuilder {}
impl InBuilder for SizeBuilder {}

#[cfg(test)]
mod tests {
    use rusoto_dynamodb::AttributeValue;

    use crate::*;

    #[test]
    fn name_equal_name() -> anyhow::Result<()> {
        let input = name("foo").equal(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n")
                ],
                "$c = $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_equal_value() -> anyhow::Result<()> {
        let input = value(5).equal(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                ],
                "$c = $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_equal_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().equal(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c = $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_not_equal_name() -> anyhow::Result<()> {
        let input = name("foo").not_equal(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c <> $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_not_equal_value() -> anyhow::Result<()> {
        let input = value(5).not_equal(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c <> $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_not_equal_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().not_equal(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c <> $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_less_than_name() -> anyhow::Result<()> {
        let input = name("foo").less_than(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c < $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_less_than_value() -> anyhow::Result<()> {
        let input = value(5).less_than(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c < $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_less_than_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().less_than(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c < $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_less_than_equal_name() -> anyhow::Result<()> {
        let input = name("foo").less_than_equal(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c <= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_less_than_equal_value() -> anyhow::Result<()> {
        let input = value(5).less_than_equal(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c <= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_less_than_equal_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().less_than_equal(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c <= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_greater_than_name() -> anyhow::Result<()> {
        let input = name("foo").greater_than(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c > $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_greater_than_value() -> anyhow::Result<()> {
        let input = value(5).greater_than(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c > $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_greater_than_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().greater_than(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c > $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_greater_than_equal_name() -> anyhow::Result<()> {
        let input = name("foo").greater_than_equal(name("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                ],
                "$c >= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn value_greater_than_equal_value() -> anyhow::Result<()> {
        let input = value(5).greater_than_equal(value("bar"));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some(5.to_string()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c >= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn name_size_greater_than_equal_name_size() -> anyhow::Result<()> {
        let input = name("foo[1]").size().greater_than_equal(name("bar").size());

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n[1])"),
                    ExpressionNode::from_names(vec!["bar".to_owned()], "size ($n)"),
                ],
                "$c >= $c"
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_operand_error_equal() -> anyhow::Result<()> {
        let input = name("").size().equal(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_not_equal() -> anyhow::Result<()> {
        let input = name("").size().not_equal(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_less_than() -> anyhow::Result<()> {
        let input = name("").size().less_than(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_less_than_equal() -> anyhow::Result<()> {
        let input = name("").size().less_than_equal(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_greater_than() -> anyhow::Result<()> {
        let input = name("").size().greater_than(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_greater_than_equal() -> anyhow::Result<()> {
        let input = name("").size().greater_than_equal(value(5));

        assert_eq!(
            input
                .build_tree()
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
    fn no_match_error() -> anyhow::Result<()> {
        let input = ConditionBuilder::default();

        assert_eq!(
            input
                .build_tree()
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
    fn basic_method_and() -> anyhow::Result<()> {
        let input = name("foo")
            .equal(value(5))
            .and(name("bar").equal(value("baz")));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_children_expression(
                        vec![
                            ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                            ExpressionNode::from_values(
                                vec![AttributeValue {
                                    n: Some("5".to_owned()),
                                    ..Default::default()
                                }],
                                "$v"
                            )
                        ],
                        "$c = $c"
                    ),
                    ExpressionNode::from_children_expression(
                        vec![
                            ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                            ExpressionNode::from_values(
                                vec![AttributeValue {
                                    s: Some("baz".to_owned()),
                                    ..Default::default()
                                }],
                                "$v"
                            )
                        ],
                        "$c = $c"
                    ),
                ],
                "($c) AND ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_or() -> anyhow::Result<()> {
        let input = name("foo")
            .equal(value(5))
            .or(name("bar").equal(value("baz")));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_children_expression(
                        vec![
                            ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                            ExpressionNode::from_values(
                                vec![AttributeValue {
                                    n: Some("5".to_owned()),
                                    ..Default::default()
                                }],
                                "$v"
                            )
                        ],
                        "$c = $c"
                    ),
                    ExpressionNode::from_children_expression(
                        vec![
                            ExpressionNode::from_names(vec!["bar".to_owned()], "$n"),
                            ExpressionNode::from_values(
                                vec![AttributeValue {
                                    s: Some("baz".to_owned()),
                                    ..Default::default()
                                }],
                                "$v"
                            )
                        ],
                        "$c = $c"
                    ),
                ],
                "($c) OR ($c)"
            )
        );

        Ok(())
    }

    // TODO: variadic tests require a macro with variadic arguments

    #[test]
    fn invalid_operand_error_and() -> anyhow::Result<()> {
        let input = name("")
            .size()
            .greater_than_equal(value(5))
            .and(name("[5]").between(value(3), value(9)));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_or() -> anyhow::Result<()> {
        let input = name("")
            .size()
            .greater_than_equal(value(5))
            .or(name("[5]").between(value(3), value(9)));

        assert_eq!(
            input
                .build_tree()
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
    fn basic_method_not() -> anyhow::Result<()> {
        let input = name("foo").equal(value(5)).not();

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![ExpressionNode::from_children_expression(
                    vec![
                        ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                        ExpressionNode::from_values(
                            vec![AttributeValue {
                                n: Some("5".to_owned()),
                                ..Default::default()
                            }],
                            "$v"
                        )
                    ],
                    "$c = $c"
                )],
                "NOT ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_function_not() -> anyhow::Result<()> {
        let input = not(name("foo").equal(value(5)));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![ExpressionNode::from_children_expression(
                    vec![
                        ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                        ExpressionNode::from_values(
                            vec![AttributeValue {
                                n: Some("5".to_owned()),
                                ..Default::default()
                            }],
                            "$v"
                        )
                    ],
                    "$c = $c"
                )],
                "NOT ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_operand_error_not() -> anyhow::Result<()> {
        let input = name("")
            .size()
            .greater_than_equal(value(5))
            .or(name("[5]").between(value(3), value(9)))
            .not();

        assert_eq!(
            input
                .build_tree()
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
    fn basic_method_between_for_name() -> anyhow::Result<()> {
        let input = name("foo").between(value(5), value(7));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c BETWEEN $c AND $c"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_between_for_value() -> anyhow::Result<()> {
        let input = value(6).between(value(5), value(7));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("6".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c BETWEEN $c AND $c"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_between_for_size() -> anyhow::Result<()> {
        let input = name("foo").size().between(value(5), value(7));

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n)"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c BETWEEN $c AND $c"
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_operand_error_between() -> anyhow::Result<()> {
        let input = name("[5]").between(value(3), name("foo..bar"));

        assert_eq!(
            input
                .build_tree()
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
    fn basic_method_in_for_name() -> anyhow::Result<()> {
        let input = name("foo").r#in(vec![value(5), value(7)]);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c IN ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_in_for_value() -> anyhow::Result<()> {
        let input = value(6).r#in(vec![value(5), value(7)]);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("6".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c IN ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_method_in_for_size() -> anyhow::Result<()> {
        let input = name("foo").size().r#in(vec![value(5), value(7)]);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "size ($n)"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("5".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    ),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            n: Some("7".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "$c IN ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_operand_error_in() -> anyhow::Result<()> {
        let input = name("[5]").r#in(vec![value(3), name("foo..bar")]);

        assert_eq!(
            input
                .build_tree()
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
    fn basic_attr_exists() -> anyhow::Result<()> {
        let input = name("foo").attribute_exists();

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![ExpressionNode::from_names(vec!["foo".to_owned()], "$n")],
                "attribute_exists ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn basic_attr_not_exists() -> anyhow::Result<()> {
        let input = name("foo").attribute_not_exists();

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![ExpressionNode::from_names(vec!["foo".to_owned()], "$n")],
                "attribute_not_exists ($c)"
            )
        );

        Ok(())
    }

    #[test]
    fn invalid_operand_error_attr_exists() -> anyhow::Result<()> {
        let input = attribute_exists(name(""));

        assert_eq!(
            input
                .build_tree()
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
    fn invalid_operand_error_not_attr_exists() -> anyhow::Result<()> {
        let input = attribute_not_exists(name(""));

        assert_eq!(
            input
                .build_tree()
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
    fn attr_type_string() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDbAttributeType::String);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("S".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_stringset() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDbAttributeType::StringSet);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("SS".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_number() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDbAttributeType::Number);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("N".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_binaryset() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDbAttributeType::BinarySet);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("BS".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_boolean() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDbAttributeType::Boolean);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("BOOL".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_null() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDbAttributeType::Null);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("NULL".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_list() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDbAttributeType::List);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("L".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_map() -> anyhow::Result<()> {
        let input = name("foo").attribute_type(DynamoDbAttributeType::Map);

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("M".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "attribute_type ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn attr_type_invalid_operand() -> anyhow::Result<()> {
        let input = name("").attribute_type(DynamoDbAttributeType::Map);

        assert_eq!(
            input
                .build_tree()
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
    fn basic_begins_with() -> anyhow::Result<()> {
        let input = name("foo").begins_with("bar");

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "begins_with ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn begins_with_invalid_operand() -> anyhow::Result<()> {
        let input = name("").begins_with("bar");

        assert_eq!(
            input
                .build_tree()
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
    fn basic_contains() -> anyhow::Result<()> {
        let input = name("foo").contains("bar");

        assert_eq!(
            input.build_tree()?,
            ExpressionNode::from_children_expression(
                vec![
                    ExpressionNode::from_names(vec!["foo".to_owned()], "$n"),
                    ExpressionNode::from_values(
                        vec![AttributeValue {
                            s: Some("bar".to_owned()),
                            ..Default::default()
                        }],
                        "$v"
                    )
                ],
                "contains ($c, $c)"
            )
        );

        Ok(())
    }

    #[test]
    fn contains_invalid_operand() -> anyhow::Result<()> {
        let input = name("").contains("bar");

        assert_eq!(
            input
                .build_tree()
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
    fn compound_and() -> anyhow::Result<()> {
        let input = ConditionBuilder {
            condition_list: vec![
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
            ],
            mode: condition::ConditionMode::And,
            ..Default::default()
        };

        assert_eq!(
            ConditionBuilder::compound_build_condition(&input, ExpressionNode::default())?
                .fmt_expression,
            "($c) AND ($c) AND ($c) AND ($c)",
        );

        Ok(())
    }

    #[test]
    fn compound_or() -> anyhow::Result<()> {
        let input = ConditionBuilder {
            condition_list: vec![
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
                ConditionBuilder::default(),
            ],
            mode: condition::ConditionMode::Or,
            ..Default::default()
        };

        assert_eq!(
            ConditionBuilder::compound_build_condition(&input, ExpressionNode::default())?
                .fmt_expression,
            "($c) OR ($c) OR ($c) OR ($c) OR ($c) OR ($c) OR ($c)",
        );

        Ok(())
    }

    #[test]
    fn in_and() -> anyhow::Result<()> {
        let input = ConditionBuilder {
            operand_list: vec![
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
                Box::new(NameBuilder::default()),
            ],
            mode: condition::ConditionMode::And,
            ..Default::default()
        };

        assert_eq!(
            ConditionBuilder::in_build_condition(&input, ExpressionNode::default()).fmt_expression,
            "$c IN ($c, $c, $c, $c, $c, $c)",
        );

        Ok(())
    }
}
