//! Ported from [error.go](https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/error.go)

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ExpressionError {
    #[error("{0} error: invalid parameter: {1}")]
    InvalidParameterError(/*functionName*/ String, /*parameterType*/ String),

    #[error("{0} error: unset parameter: {1}")]
    UnsetParameterError(/*functionName*/ String, /*parameterType*/ String),
}

#[cfg(test)]
mod tests {
    use crate::error::ExpressionError;

    #[test]
    fn invalid_error() -> anyhow::Result<()> {
        let input = ExpressionError::InvalidParameterError("func".to_owned(), "param".to_owned());

        assert_eq!(format!("{}", input), "func error: invalid parameter: param");

        Ok(())
    }

    #[test]
    fn unset_error() -> anyhow::Result<()> {
        let input = ExpressionError::UnsetParameterError("func".to_owned(), "param".to_owned());

        assert_eq!(format!("{}", input), "func error: unset parameter: param");

        Ok(())
    }
}
