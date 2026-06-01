/// @author Oleg
/// time-start: ???
/// time-end: ???
/// 
/// For my ISU stage 5, I did not like any of the options, 
/// because I wanted to craete something functional, 
/// something that can have real use.
/// So I decided to make a websocket chat.
/// 
/// I am technicaly using queues, but it is happening implicitly behind the scenes,
/// rather than direcly using a queues object.
/// every time in my code you see ".await" rust places the funcion into a buffer queue.
/// 
/// for example:
/// receiver.next().await 
/// When a client sends 5 chat messages rapidly,
/// Rust doesn't process them all at the exact same millisecond,
/// instead it places them all into a buffer queue.
/// 
/// while let Some(Ok(msg)) = receiver.next().await
/// is pulling messages out of that incoming queue one by one.


use std::{collections::HashMap, sync::{Arc, atomic::{AtomicUsize, Ordering}}};

use axum::{Router, extract::{State, WebSocketUpgrade, ws::{Message, WebSocket}}, response::{Html, IntoResponse}, routing::get};

use futures_util::{SinkExt, StreamExt, lock::Mutex, stream::SplitSink};
use serde::{Deserialize, Serialize};

// UUID for my chat, I need to be able to diffirintiate between clients
static NEXT_CLIENT_ID: AtomicUsize = AtomicUsize::new(1);

// Stucture for the message, 
// I will use this with json to convert string from client
// into a message structure then modify it and then send back
#[derive(Serialize, Deserialize, Debug)]
struct UserMessage {
    user: String,
    text: String,
    time: Option<String>,
    user_id: Option<usize>,
}

// tokio is the multithreading network library for rust
#[tokio::main]
async fn main() {
    // This object looks complicated, but it is actuacly pretty simple
    // Arc means that I can access this object between threads
    // Mutex means that I can modify it between threads
    let clients = Arc::new(Mutex::new(HashMap::<usize, SplitSink<WebSocket, Message>>::new()));

    let address = "0.0.0.0:8085";

    // all the paths my server handles
    // for tokio to give some kind of variable to a handler it has to be a state.
    let app: Router = Router::new()
    .route("/", get(main_page_handler))
    .route("/styles.css", get(styles_handler))
    .route("/ws", get(ws_handle))
    .with_state(clients);

    // you will see .unwrap() a whole bunch of times
    // it is a very simple version of try and catch
    // works like this:
    // run this code, if it works great! if it does not crush the program.

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


#[cfg(not(debug_assertions))]
async fn styles_handler() -> impl IntoResponse{
    const STYLES_CSS: &str = include_str!("../res/styles.html");
    ([("content-type", "text/css")], STYLES_CSS)
}

#[cfg(debug_assertions)]
async fn styles_handler() -> impl IntoResponse{
    let styles_css = tokio::fs::read_to_string("res/styles.css").await.unwrap();
    ([("content-type", "text/css")], styles_css)
}

// standart http to websocket upgrade
async fn ws_handle(ws: WebSocketUpgrade, clients: State<Arc<Mutex<HashMap<usize, SplitSink<WebSocket, Message>>>>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, clients))
}

async fn handle_socket(socket: WebSocket, State(clients): State<Arc<Mutex<HashMap<usize, SplitSink<WebSocket, Message>>>>>) {
    // fetch_add is specificaly designed for a multithreaded UUID getter
    // get a unique id and incease it by one, no user will have the same id
    let current_client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    
    println!("client connected with id {}", current_client_id);

    let (sender, mut receiver) = socket.split();
    // this is a temporary block scope to modify the client list
    // this program is mutlithreader, if one thread modifies an variable 
    // while other tries to read it, it can cause a lot complicated errors
    // this code "locks" the client list to this thread, modifies it and then unlocks it
    // nobody else can interact with it while this is happening
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
                    // same logic from before
                    // lock
                    let mut clients_guard = clients.lock().await;
                    println!("there a lot of you: {}", clients_guard.len());

                    if let Ok(mut original_message) = serde_json::from_str::<UserMessage>(&text.to_string()) {
                        let now = chrono::Local::now();
                        let formated_time = now.format("%H:%M:%S").to_string();
                        // set the message time and user_id, client should not handle this information
                        original_message.time = Some(formated_time);
                        original_message.user_id = Some(current_client_id);
                                                
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
                    // unlock
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

    // same logic as before, but for removing a client
    {
        let mut clients_guard = clients.lock().await;
        clients_guard.remove(&current_client_id);
        drop(clients_guard);
    }

    println!("goodbye {}", current_client_id);
}