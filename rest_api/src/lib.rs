#[macro_use]
extern crate rocket;
mod logging;
mod auth;

use rocket::{serde::json::Json, Build, Rocket};
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

// #[launch]
pub fn rocket() -> Rocket<Build> {
    rocket
        ::build()
        .attach(logging::LoggingFairing)
        .attach(auth::AuthFairing)
        .mount("/", routes![index, path, post])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = path("test");
        assert_eq!(result, "test");
    }
}
