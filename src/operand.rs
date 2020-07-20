pub struct Operand {}

pub trait OperandBuilder {
    fn build_operand() -> anyhow::Result<Operand>;
}

pub struct ValueBuilder<T> {
    value: T,
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

pub fn name<S>(name: S) -> NameBuilder
where
    S: Into<String>,
{
    NameBuilder { name: name.into() }
}

impl OperandBuilder for NameBuilder {
    fn build_operand() -> anyhow::Result<Operand> {
        Ok(Operand {})
    }
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
