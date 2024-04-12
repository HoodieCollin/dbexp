

#[macro_use]
extern crate rocket;
mod logging;
mod auth;


#[get("/foo")]
fn foo() -> &'static str {
    "foo/index"
}

#[get("/foo/<bar>")]
fn foobar(bar: &str) -> String {
    format!("foo/{}", bar)
}

#[launch]
fn rocket() -> _ {
    rocket
        ::build()
        .attach(logging::LoggingFairing)
        .attach(auth::AuthFairing)
        .mount("/", routes![foo, foobar])
}
