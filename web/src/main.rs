mod api;
mod config;
mod client;
mod wss;
mod web_error;

use actix_web::{web, App, HttpServer, Error, Responder, HttpResponse, middleware, HttpRequest};
use actix::{Actor, Addr, StreamHandler};
use actix_files::Files;
use actix_web_actors::ws;
use actix_web_actors::ws::WsResponseBuilder;
use common::time_util::now_str;
use common::uuid::uuid_timestamp;

use crate::api::default::{data, index, favicon, get_static_dir};
use crate::config::constant::CFG;
use crate::wss::server::{MyWebSocket, SendMessage, WsContext};
use crate::wss::session_manager::SessionManager;

/// WebSocket handshake and start `MyWebSocket` actor.
async fn index_echo_ws(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    let session_id = get_session_id(&req, uuid_timestamp());

    let build = WsResponseBuilder::new(MyWebSocket::new(Some(session_id.clone())), &req, stream);
    let resp = build.start_with_addr();

    match resp {
        Ok((addr, resp)) => {
            let context = WsContext::new(addr, session_id.clone(), now_str());
            context.do_send("Binlog Server 连接成功");

            SessionManager::ws_insertupdate(session_id, context);

            Ok(resp)
        }
        Err(err) => {
            Err(err)
        }
    }
}

// #[actix_web::main]
#[tokio::main]
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

fn get_session_id(req: &HttpRequest, default:String) -> String {
    // 从HTTP头中获取sessionId
    let mut session_id = req.headers().get("X-Session-Id").and_then(|h| h.to_str().ok()).unwrap_or_default();

    if session_id.is_empty() {
        session_id = default.as_str();
    }

    return session_id.to_string();
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        assert_eq!(1, 1);
        println!("binlog lib test:{}", 0x21);
    }
}
