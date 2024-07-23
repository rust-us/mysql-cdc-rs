use serde::{Deserialize, Serialize};
use crate::err::decode_error::ReError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoadStyle {
    DEFAULT,

    YAML,

}

#[derive(Debug, Clone, Serialize)]
pub enum Format {
    Json,

    Yaml,

    None,
}

impl TryFrom<&str> for Format {
    type Error = (ReError);

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "yaml" => {
                Ok(Format::Yaml)
            },
            "json" => {
                Ok(Format::Json)
            },
            _ => {
                Err(ReError::String(String::from("Format error")))
            }
        }
    }
}

impl Format {
    pub fn format(format: &String) -> Format {
        let f =  match format {
            ff => {
                let f = Format::try_from(ff.as_str());

                match f {
                    Ok(fff) => {
                        fff
                    },
                    Err(e) => {
                        Format::Yaml
                    }
                }
            }
        };

        return f;
    }

}