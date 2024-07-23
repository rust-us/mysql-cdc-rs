mod api;
mod config;
mod client;
mod wss;
mod web_error;

use actix_web::{web, App, HttpServer, Error, Responder, HttpResponse, middleware, HttpRequest};
use actix::{Actor, StreamHandler};
use actix_files::Files;
use actix_web_actors::ws;

use crate::api::default::{data, index, favicon, get_static_dir};
use crate::config::constant::CFG;
use crate::wss::server::MyWebSocket;

/// WebSocket handshake and start `MyWebSocket` actor.
async fn index_echo_ws(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    let resp = ws::start(MyWebSocket::new(), &req, stream);

    println!("{:?}", resp);
    resp
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let host = CFG.get("ADDRESS").unwrap();
    let port = CFG.get("PORT").unwrap();

    log::info!("{}", format!("starting HTTP server at http://{}:{}", &host, &port));

    HttpServer::new(move || {
        App::new()
            // 将"/static"前缀映射到"./static"目录
            // 作为服务（service）被添加到应用中，而不是通过 .wrap() 方法。这是因为 Files 是一个完整的服务，它处理以 /static 开头的所有请求，并将它们映射到文件系统的 ./static 目录中
            .service(Files::new("/static", format!("{}/static", get_static_dir())))
            // .route("/", HttpMethod::Get, |_| HttpResponse::Ok().body("Hello, Rust Web!"))
            .service(index)
            .service(data)
            .service(web::resource("/favicon").to(favicon))
            // websocket route
            .service(web::resource("/ws").route(web::get().to(index_echo_ws)))
            // enable logger
            .wrap(middleware::Logger::default())
    })
        .workers(2)
        .bind(format!("{}:{}", host, port))?
        .run()
        .await
}


#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
        println!("binlog lib test:{}", 0x21);
    }
}
