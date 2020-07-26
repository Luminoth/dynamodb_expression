// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/operand.go

pub struct Operand {
    //exprNode: ExpressionNode,
}

pub trait OperandBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand>;

    fn into_boxed(self) -> Box<Self>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

pub enum ValueBuilder {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl OperandBuilder for ValueBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        Ok(Operand {})
    }
}

impl PlusBuilder for ValueBuilder {}
impl MinusBuilder for ValueBuilder {}
impl ListAppendBuilder for ValueBuilder {}

pub fn bool_value(value: bool) -> Box<ValueBuilder> {
    ValueBuilder::Bool(value).into_boxed()
}

pub fn int_value(value: i64) -> Box<ValueBuilder> {
    ValueBuilder::Int(value).into_boxed()
}

pub fn float_value(value: f64) -> Box<ValueBuilder> {
    ValueBuilder::Float(value).into_boxed()
}

pub fn string_value(value: impl Into<String>) -> Box<ValueBuilder> {
    ValueBuilder::String(value.into()).into_boxed()
}

pub struct NameBuilder {
    name: String,
}

impl NameBuilder {
    pub fn size(self: Box<Self>) -> Box<SizeBuilder> {
        SizeBuilder { name_builder: self }.into_boxed()
    }

    pub fn if_not_exists(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder> {
        if_not_exists(self, right)
    }
}

impl OperandBuilder for NameBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        Ok(Operand {})
    }
}

impl PlusBuilder for NameBuilder {}
impl MinusBuilder for NameBuilder {}
impl ListAppendBuilder for NameBuilder {}

pub fn name(name: impl Into<String>) -> Box<NameBuilder> {
    NameBuilder { name: name.into() }.into_boxed()
}

pub struct SizeBuilder {
    name_builder: Box<NameBuilder>,
}

impl OperandBuilder for SizeBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        Ok(Operand {})
    }
}

pub fn size(name_builder: Box<NameBuilder>) -> Box<SizeBuilder> {
    name_builder.size()
}

pub struct KeyBuilder {
    key: String,
}

impl OperandBuilder for KeyBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        Ok(Operand {})
    }
}

pub fn key(key: impl Into<String>) -> Box<KeyBuilder> {
    KeyBuilder { key: key.into() }.into_boxed()
}

enum SetValueMode {
    //Unset,
    Plus,
    Minus,
    ListAppend,
    IfNotExists,
}

trait PlusBuilder: OperandBuilder {
    fn plus(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder>
    where
        Self: Sized + 'static,
    {
        plus(self, right)
    }
}

trait MinusBuilder: OperandBuilder {
    fn minus(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder>
    where
        Self: Sized + 'static,
    {
        minus(self, right)
    }
}

trait ListAppendBuilder: OperandBuilder {
    fn list_append(self: Box<Self>, right: Box<dyn OperandBuilder>) -> Box<SetValueBuilder>
    where
        Self: Sized + 'static,
    {
        list_append(self, right)
    }
}

pub struct SetValueBuilder {
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
    mode: SetValueMode,
}

impl OperandBuilder for SetValueBuilder {
    fn build_operand(&self) -> anyhow::Result<Operand> {
        Ok(Operand {})
    }
}

pub fn plus(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::Plus,
    }
    .into_boxed()
}

pub fn minus(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::Minus,
    }
    .into_boxed()
}

pub fn list_append(
    left_operand: Box<dyn OperandBuilder>,
    right_operand: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::ListAppend,
    }
    .into_boxed()
}

pub fn if_not_exists(
    name: Box<NameBuilder>,
    value: Box<dyn OperandBuilder>,
) -> Box<SetValueBuilder> {
    SetValueBuilder {
        left_operand: name.into_boxed(),
        right_operand: value,
        mode: SetValueMode::IfNotExists,
    }
    .into_boxed()
}

#[cfg(test)]
fn test_name_builder() {
    let builder = name("test");
}

#[cfg(test)]
fn test_value_builder() {
    let builder = ValueBuilder::String("test".to_owned());
}

#[cfg(test)]
fn test_key_builder() {
    let builder = key("test");
}

#[cfg(test)]
fn test_size_builder() {
    let builder = size(name("test"));

    let builder = name("test").size();
}

#[cfg(test)]
fn test_size_builder_plus() {
    // TODO: set()

    let expr = plus(int_value(10), int_value(5));

    let expr = name("test").plus(int_value(10));

    let expr = int_value(10).plus(int_value(5));
}

#[cfg(test)]
fn test_size_builder_minus() {
    // TODO: set()

    let expr = minus(int_value(10), int_value(5));

    let expr = name("test").minus(int_value(10));

    let expr = int_value(10).minus(int_value(5));
}

#[cfg(test)]
fn test_size_builder_list_append() {
    // TODO: set()

    let expr = list_append(int_value(10), int_value(5));

    let expr = name("test").list_append(int_value(10));

    let expr = int_value(10).list_append(int_value(5));
}

#[cfg(test)]
fn test_size_builder_if_not_exists() {
    // TODO: set()

    let expr = if_not_exists(name("test"), int_value(0));

    let expr = name("test").if_not_exists(int_value(10));
}
