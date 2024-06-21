use std::io;
use std::io::{Cursor, Write};
use byteorder::WriteBytesExt;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use crate::declar::auth_plugin_names::AuthPlugin;
use crate::NULL_TERMINATOR;

pub fn write_null_term_string(
    cursor: &mut Cursor<&mut Vec<u8>>,
    str: &String) -> Result<(), io::Error> {
    cursor.write(str.as_bytes())?;
    cursor.write_u8(NULL_TERMINATOR)?;

    Ok(())
}

pub fn encrypt_password(password: &String, scramble: &String, auth_plugin: &AuthPlugin) -> Vec<u8> {
    match auth_plugin {
        AuthPlugin::MySqlNativePassword => {
            let password_hash = sha1(password.as_bytes());
            let concat_hash = [scramble.as_bytes().to_vec(), sha1(&password_hash)].concat();
            xor(&password_hash, &sha1(&concat_hash))
        }
        AuthPlugin::CachingSha2Password => {
            let password_hash = sha256(password.as_bytes());
            let concat_hash = [scramble.as_bytes().to_vec(), sha256(&password_hash)].concat();
            xor(&password_hash, &sha256(&concat_hash))
        }
    }
}

pub fn xor(slice1: &[u8], slice2: &[u8]) -> Vec<u8> {
    let mut result = vec![0u8; slice1.len()];
    for i in 0..result.len() {
        result[i] = slice1[i] ^ slice2[i % slice2.len()];
    }
    result
}

pub fn sha1(value: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(value);
    hasher.finalize().as_slice().to_vec()
}

pub fn sha256(value: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(value);
    hasher.finalize().as_slice().to_vec()
}
