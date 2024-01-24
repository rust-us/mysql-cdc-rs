use std::io;
use std::io::{Cursor, Write};
use crate::bytes::encrypt_password;
use crate::declar::auth_plugin_names::AuthPlugin;

pub struct AuthPluginSwitchCommand {
    pub password: String,
    pub scramble: String,
    pub auth_plugin_name: String,
    pub auth_plugin: AuthPlugin,
}

impl AuthPluginSwitchCommand {
    pub fn new(
        password: &String,
        scramble: &String,
        auth_plugin_name: &String,
        auth_plugin: AuthPlugin,
    ) -> Self {
        Self {
            password: password.clone(),
            scramble: scramble.clone(),
            auth_plugin_name: auth_plugin_name.clone(),
            auth_plugin,
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, io::Error> {
        let mut vec = Vec::new();
        let mut cursor = Cursor::new(&mut vec);

        let encrypted_password =
            encrypt_password(&self.password, &self.scramble, &self.auth_plugin);
        cursor.write(&encrypted_password)?;

        Ok(vec)
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
        println!("binlog lib test:{}", 0x21);
    }
}
