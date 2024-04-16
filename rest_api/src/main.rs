#[macro_use]
extern crate rocket;
mod logging;
mod auth;

use rocket::serde::json::Json;
use serde::Deserialize;


// simple get
#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

// path parameters
#[get("/<foo>")]
fn path(foo: &str) -> String {
    foo.to_string()
}

// post body
#[derive(Deserialize)]
struct PostBody {
    foo: String,
    bar: bool,
}

#[post("/foo", format = "json", data = "<body>")]
fn post(body: Json<PostBody>) {
    println!("foo: {}, bar: {}", body.foo, body.bar);
}

#[launch]
fn rocket() -> _ {
    rocket
        ::build()
        .attach(logging::LoggingFairing)
        .attach(auth::AuthFairing)
        .mount("/", routes![index, path, post])
}
