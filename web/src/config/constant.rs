use lazy_static::lazy_static;
use std::collections::HashMap;

lazy_static! {

    pub static ref CFG: HashMap<&'static str, String> = {
        let mut map = HashMap::new();

        map.insert(
            "ADDRESS",
            String::from("127.0.0.1"),
        );
        map.insert(
            "PORT",
            String::from("8080"),
        );

        map
    };
}
