// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/operand.go

pub struct Operand {
    //exprNode: ExpressionNode,
}

pub trait OperandBuilder {
    fn build_operand() -> anyhow::Result<Operand>;
}

pub struct ValueBuilder<T> {
    value: T,
}

impl<T> ValueBuilder<T> {
    pub fn plus<R>(self, right: R) -> SetValueBuilder<ValueBuilder<T>, R>
    where
        R: OperandBuilder,
    {
        plus(self, right)
    }

    pub fn minus<R>(self, right: R) -> SetValueBuilder<ValueBuilder<T>, R>
    where
        R: OperandBuilder,
    {
        minus(self, right)
    }

    pub fn list_append<R>(self, right: R) -> SetValueBuilder<ValueBuilder<T>, R>
    where
        R: OperandBuilder,
    {
        list_append(self, right)
    }
}

impl<T> OperandBuilder for ValueBuilder<T> {
    fn build_operand() -> anyhow::Result<Operand> {
        Ok(Operand {})
    }
}

pub fn value<T>(value: T) -> ValueBuilder<T> {
    ValueBuilder { value }
}

pub struct NameBuilder {
    name: String,
}

impl NameBuilder {
    pub fn size(self) -> SizeBuilder {
        SizeBuilder { name_builder: self }
    }

    pub fn plus<R>(self, right: R) -> SetValueBuilder<NameBuilder, R>
    where
        R: OperandBuilder,
    {
        plus(self, right)
    }

    pub fn minus<R>(self, right: R) -> SetValueBuilder<NameBuilder, R>
    where
        R: OperandBuilder,
    {
        minus(self, right)
    }

    pub fn list_append<R>(self, right: R) -> SetValueBuilder<NameBuilder, R>
    where
        R: OperandBuilder,
    {
        list_append(self, right)
    }

    pub fn if_not_exists<R>(self, right: R) -> SetValueBuilder<NameBuilder, R>
    where
        R: OperandBuilder,
    {
        if_not_exists(self, right)
    }
}

impl OperandBuilder for NameBuilder {
    fn build_operand() -> anyhow::Result<Operand> {
        Ok(Operand {})
    }
}

pub fn name<S>(name: S) -> NameBuilder
where
    S: Into<String>,
{
    NameBuilder { name: name.into() }
}

pub struct SizeBuilder {
    name_builder: NameBuilder,
}

impl OperandBuilder for SizeBuilder {
    fn build_operand() -> anyhow::Result<Operand> {
        Ok(Operand {})
    }
}

pub fn size(name_builder: NameBuilder) -> SizeBuilder {
    name_builder.size()
}

pub struct KeyBuilder {
    key: String,
}

impl OperandBuilder for KeyBuilder {
    fn build_operand() -> anyhow::Result<Operand> {
        Ok(Operand {})
    }
}

pub fn key<S>(key: S) -> KeyBuilder
where
    S: Into<String>,
{
    KeyBuilder { key: key.into() }
}

enum SetValueMode {
    Unset,
    Plus,
    Minus,
    ListAppend,
    IfNotExists,
}

pub struct SetValueBuilder<L, R>
where
    L: OperandBuilder,
    R: OperandBuilder,
{
    left_operand: L,
    right_operand: R,
    mode: SetValueMode,
}

pub fn plus<L, R>(left_operand: L, right_operand: R) -> SetValueBuilder<L, R>
where
    L: OperandBuilder,
    R: OperandBuilder,
{
    SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::Plus,
    }
}

pub fn minus<L, R>(left_operand: L, right_operand: R) -> SetValueBuilder<L, R>
where
    L: OperandBuilder,
    R: OperandBuilder,
{
    SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::Minus,
    }
}

pub fn list_append<L, R>(left_operand: L, right_operand: R) -> SetValueBuilder<L, R>
where
    L: OperandBuilder,
    R: OperandBuilder,
{
    SetValueBuilder {
        left_operand,
        right_operand,
        mode: SetValueMode::ListAppend,
    }
}

pub fn if_not_exists<R>(name: NameBuilder, value: R) -> SetValueBuilder<NameBuilder, R>
where
    R: OperandBuilder,
{
    SetValueBuilder {
        left_operand: name,
        right_operand: value,
        mode: SetValueMode::IfNotExists,
    }
}

#[cfg(test)]
fn test_name_builder() {
    let builder = name("test");
}

#[cfg(test)]
fn test_value_builder() {
    let builder = value("test");
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

    let expr = plus(value(10), value(5));

    let expr = name("test").plus(value(10));

    let expr = value(10).plus(value(5));
}

#[cfg(test)]
fn test_size_builder_minus() {
    // TODO: set()

    let expr = minus(value(10), value(5));

    let expr = name("test").minus(value(10));

    let expr = value(10).minus(value(5));
}

#[cfg(test)]
fn test_size_builder_list_append() {
    // TODO: set()

    let expr = list_append(value(10), value(5));

    let expr = name("test").list_append(value(10));

    let expr = value(10).list_append(value(5));
}

#[cfg(test)]
fn test_size_builder_if_not_exists() {
    // TODO: set()

    let expr = if_not_exists(name("test"), value(0));

    let expr = name("test").if_not_exists(value(10));
}
