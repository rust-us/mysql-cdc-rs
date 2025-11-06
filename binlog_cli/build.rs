use std::env;

fn main() {
    // 设置构建时的环境变量
    println!("cargo:rerun-if-changed=build.rs");
    
    // 获取构建信息
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());
    
    println!("cargo:rustc-env=BUILD_TARGET={}", target);
    println!("cargo:rustc-env=BUILD_PROFILE={}", profile);
    
    // 如果是 release 构建，设置优化标志
    if profile == "release" {
        println!("cargo:rustc-env=CARGO_CFG_OPTIMIZED=1");
    }
}