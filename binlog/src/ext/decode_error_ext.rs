use sqlparser::parser::ParserError;
use common::err::decode_error::ReError;

pub fn decode_error_from(value: ParserError) -> ReError {
    ReError::ASTParserError(value.to_string())
}