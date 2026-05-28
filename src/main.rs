use std::{collections::HashMap, sync::{Arc, atomic::{AtomicUsize, Ordering}}};

use axum::{Router, extract::{State, WebSocketUpgrade, ws::{Message, WebSocket}}, response::{Html, IntoResponse}, routing::get};

use futures_util::{SinkExt, StreamExt, lock::Mutex, stream::SplitSink};
use serde::{Deserialize, Serialize};

static NEXT_CLIENT_ID: AtomicUsize = AtomicUsize::new(1);

#[derive(Serialize, Deserialize, Debug)]
struct UserMessage {
    user: String,
    text: String,
    time: Option<String>,
}

#[tokio::main]
async fn main() {
    let clients = Arc::new(Mutex::new(HashMap::<usize, SplitSink<WebSocket, Message>>::new()));

    let address = "0.0.0.0:8085";

    let app: Router = Router::new()
    .route("/", get(main_page_handler))
    .route("/styles.css", get(styles_handler))
    .route("/ws", get(ws_handle))
    .with_state(clients);

    let listener_future = tokio::net::TcpListener::bind(address);
    let listener = listener_future.await.unwrap();

    println!("Server started at http://{}", listener.local_addr().unwrap());
    
    axum::serve(listener, app).await.unwrap();
    
}

#[cfg(not(debug_assertions))]
async fn main_page_handler() -> Html<&'static str> {
    const INDEX_HTML: &str = include_str!("../res/index.html");
    Html(INDEX_HTML)
}

#[cfg(debug_assertions)]
async fn main_page_handler() -> Html<String> {
    let index_html = tokio::fs::read_to_string("res/index.html").await.unwrap();
    Html(index_html)
}

async fn styles_handler() -> impl IntoResponse{
    let styles_css = tokio::fs::read_to_string("res/styles.css").await.unwrap();
    ([("content-type", "text/css")], styles_css)
}

async fn ws_handle(ws: WebSocketUpgrade, clients: State<Arc<Mutex<HashMap<usize, SplitSink<WebSocket, Message>>>>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, clients))
}

async fn handle_socket(socket: WebSocket, State(clients): State<Arc<Mutex<HashMap<usize, SplitSink<WebSocket, Message>>>>>) {
    let current_client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    
    println!("client connected with id {}", current_client_id);

    let (sender, mut receiver) = socket.split();
    {
        let mut clients_guard = clients.lock().await;
        clients_guard.insert(current_client_id, sender);
        drop(clients_guard);
    }


    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Text(text) => {
                println!("message from {current_client_id}: {text}");
                {
                    let mut clients_guard = clients.lock().await;
                    println!("there a lot of you: {}", clients_guard.len());

                    if let Ok(mut original_message) = serde_json::from_str::<UserMessage>(&text.to_string()) {
                        let now = chrono::Local::now();
                        let formated_time = now.format("%H:%M:%S").to_string();
                        original_message.time = Some(formated_time);
                                                
                        if let Ok(message_string) = serde_json::to_string(&original_message) {
                            let message = Message::Text(message_string.into());
    
                            for (i,  client) in clients_guard.iter_mut() {
                                println!("Sending a message to {}", i);
                                let result = client.send(message.clone());
                                if result.await.is_err() {
                                    println!("Cound not send");
                                }
                            }
                        } else {
                            println!("Could not convert to text");
                        }


                    } else {
                        println!("Could not parse the message")
                    }


                    drop(clients_guard);
                }
            },
            Message::Close(_close_frame) => {
                println!("client disconnected with id {}", current_client_id);
            },
            _ => todo!()
            // Message::Binary(bytes) => todo!(),
            // Message::Ping(bytes) => todo!(),
            // Message::Pong(bytes) => todo!(),
        }
    }

    {
        let mut clients_guard = clients.lock().await;
        clients_guard.remove(&current_client_id);
        drop(clients_guard);
    }

    println!("goodbye {}", current_client_id);
}