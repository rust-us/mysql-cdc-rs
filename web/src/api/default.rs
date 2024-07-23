use actix_files::NamedFile;
use actix_web::{get, HttpResponse, Responder};
use crate::api::result::R;

/// http://127.0.0.1:8080/
#[get("/")]
async fn index() -> impl Responder {
    NamedFile::open_async(get_real_path("static/index.html")).await
}

/// favicon handler
pub(crate) async fn favicon() -> impl Responder {
    NamedFile::open_async(get_real_path("static/favicon.ico")).await
}

/// http://127.0.0.1:8080/data
#[get("/data")]
async fn data() -> impl Responder {
    let data = R::success("Hello from Rust server!");

    HttpResponse::Ok().json(data)
}

/// 获取静态资源的真正位置
pub fn get_real_path(path: &str) -> String {
    let static_dir = get_static_dir();

    format!("{}/{}", static_dir, path)
}

/// 计算静态资源的目录位置
pub fn get_static_dir() -> String {
    let current_dir = std::env::current_dir().unwrap();
    // println!("dir: {:?}", current_dir);

    let p = if current_dir.ends_with("web") {
        "."
    } else {
        "web"
    };

    p.to_string()
}
