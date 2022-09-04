//! Ported from [error.go](https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/expression/error.go)

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum ExpressionError {
    /// Returned if invalid parameters are encountered.
    ///
    /// This error specifically refers to situations where parameters are non-empty but
    /// have an invalid syntax/format. The error message includes the function
    /// that returned the error originally and the parameter type that was deemed
    /// invalid.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // err is of type InvalidParameterError
    /// let err = name("foo..bar").build_operand().err();
    /// ```
    #[error("{0} error: invalid parameter: {1}")]
    InvalidParameterError(/*functionName*/ String, /*parameterType*/ String),

    /// Returned if parameters are empty and uninitialized.
    ///
    /// This error is returned if opaque structs (ConditionBuilder, NameBuilder,
    /// Builder, etc) are initialized outside of functions in the package, since all
    /// structs in the package are designed to be initialized with functions.
    ///
    /// # Example
    ///
    /// ```
    /// use dynamodb_expression::*;
    ///
    /// // err is of type UnsetParameterError
    /// let err = Builder::default().build().err();
    /// let err = Builder::new().with_condition(ConditionBuilder::default()).build().err();
    /// ```
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
