use rocket::fairing::{ Fairing, Info, Kind };
use rocket::request::Request;
use rocket::response::Response;

pub struct LoggingFairing;

#[rocket::async_trait]
impl Fairing for LoggingFairing {
    fn info(&self) -> Info {
        Info {
            name: "Logging Fairing",
            kind: Kind::Request | Kind::Response,
        }
    }

    async fn on_request(&self, request: &mut Request<'_>, _data: &mut rocket::Data<'_>) {
        println!("<- Incoming request: {}", request.uri());
        // You can perform additional logging or processing here for incoming requests.
    }

    async fn on_response<'r>(&self, _request: &'r Request<'_>, response: &mut Response<'r>) {
        println!("-> Outgoing response: {}", response.status());
        // You can perform additional logging or processing here for outgoing responses.
    }
}
